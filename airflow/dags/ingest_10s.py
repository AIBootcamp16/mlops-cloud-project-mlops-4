from __future__ import annotations
import time
import httpx
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from common.utils import now_utc, ymd, hh, env, join_prefix
from common.s3io import atomic_commit_parquet, head_object
from common.manifest import Manifest, put_manifest

# ===== 설정: ENV =====
S3_BUCKET      = env("S3_BUCKET", required=True)
ROOT_PREFIX   = env("ROOT_PREFIX", default=None)
SERVICE_NAME   = env("SERVICE_NAME", default="upbit_ticker")  # 예: "upbit_ticker"
API_BASE_URL   = env("API_BASE_URL", default="https://api.upbit.com/v1/ticker")
MARKETS        = env("MARKETS", default="KRW-BTC,KRW-ETH,KRW-DOGE,KRW-XRP")
TARGET_FILE_MB = int(env("TARGET_FILE_MB", "192"))
SCHEMA_VERSION = int(env("RAW_SCHEMA_VERSION", "1"))

def staging_prefix(dt: datetime):
    _dt, _h = ymd(dt), hh(dt)
    path = f"staging/{SERVICE_NAME}/dt={_dt}/hour={_h}"
    return join_prefix(ROOT_PREFIX, path)

def fetch_once(client: httpx.Client) -> list[dict]:
    """
    Upbit /v1/ticker?markets=... 호출 → list[dict]
    """
    url = f"{API_BASE_URL}?markets={MARKETS}"
    r = client.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        data = [data]

    now_iso = now_utc().isoformat()
    rows: list[dict] = []
    for item in data:
        trade_ts_ms = item.get("trade_timestamp") or item.get("timestamp")
        # event_timestamp: UTC ISO, event_timestamp_ms: int(ms)
        event_ts_iso = None
        if trade_ts_ms is not None:
            event_ts_iso = datetime.utcfromtimestamp(int(trade_ts_ms) / 1000.0).isoformat() + "Z"

        # 자연키: market + trade_timestamp(ms)
        natural_key = None
        if item.get("market") and trade_ts_ms is not None:
            natural_key = f"{item['market']}::{trade_ts_ms}"

        rows.append({
            **item,  # 원형 보존
            "ingested_at": now_iso,
            "event_timestamp": event_ts_iso,
            "event_timestamp_ms": trade_ts_ms,
            "natural_key": natural_key,
            "source": "upbit-ticker",
        })
    return rows

@dag(
    dag_id="ingest_10s",
    schedule="* * * * *",  # 매 분
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["ingest", "staging", "parquet", "upbit"],
)
def ingest_10s():
    @task
    def poll_and_write():
        # ✅ 파티션 기준 시각을 KST로 통일
        t0_kst = pendulum.now("Asia/Seoul")
        prefix = staging_prefix(t0_kst)
        rows_all: list[dict] = []

        with httpx.Client() as client:
            for _ in range(6):
                try:
                    rows = fetch_once(client)
                    rows_all.extend(rows)
                except Exception as e:
                    print(f"[ingest] fetch_once failed: {e}")
                time.sleep(10)

        if not rows_all:
            print("[ingest] no rows collected this minute")
            return {"files": [], "row_count": 0, "byte_size": 0,
                    "dt": t0_kst.format("YYYY-MM-DD"), "hour": t0_kst.format("HH")}

        final_key = atomic_commit_parquet(S3_BUCKET, prefix, rows_all)
        meta = head_object(S3_BUCKET, final_key) if final_key else None
        byte_size = meta["ContentLength"] if meta else 0
        print(f"[ingest] committed: s3://{S3_BUCKET}/{final_key}, rows={len(rows_all)}, bytes={byte_size}")

        return {
            "files": [final_key] if final_key else [],
            "row_count": len(rows_all),
            "byte_size": byte_size,
            "dt": t0_kst.format("YYYY-MM-DD"),
            "hour": t0_kst.format("HH"),
        }

    @task
    def write_manifest(ret: dict):
        if not ret or not ret.get("files"):
            return
        m = Manifest(
            layer="staging",
            name=SERVICE_NAME,
            dt=ret["dt"],
            hour=ret["hour"],
            files=ret["files"],
            row_count=ret["row_count"],
            byte_size=ret["byte_size"],
            min_event_ts=None,   # 필요시 계산 가능
            max_event_ts=None,
            schema_version=SCHEMA_VERSION,
            committed_at=now_utc().isoformat(),
        )
        key = put_manifest(S3_BUCKET, m)
        print(f"[ingest] manifest written: s3://{S3_BUCKET}/{key}")

    info = poll_and_write()
    write_manifest(info)

ingest_10s()

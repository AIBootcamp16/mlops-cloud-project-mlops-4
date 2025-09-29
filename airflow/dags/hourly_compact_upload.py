from __future__ import annotations
from datetime import timedelta
import pendulum
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from airflow.decorators import dag, task
from common.utils import env, ymd, hh, join_prefix
from common.s3io import list_prefix, atomic_commit_parquet, head_object
from common.manifest import Manifest, put_manifest

S3_BUCKET      = env("S3_BUCKET", required=True)
ROOT_PREFIX   = env("ROOT_PREFIX", default=None)
PROJECT_NAME   = env("PROJECT_NAME", default="upbit_ticker_curated")
RAW_SERVICE    = env("SERVICE_NAME", default="upbit_ticker")
SCHEMA_VERSION = int(env("CURATED_SCHEMA_VERSION", "1"))

def staging_root(dt, h):
    return join_prefix(ROOT_PREFIX, f"staging/{RAW_SERVICE}/dt={dt}/hour={h}")

def curated_prefix(dt, h):
    return join_prefix(ROOT_PREFIX, f"data/{PROJECT_NAME}/dt={dt}/hour={h}")

START_DATE_KST = pendulum.now("Asia/Seoul").replace(minute=0, second=0, microsecond=0)

def _to_kst(dt) -> pendulum.DateTime:
    """
    logical_date가 None/str/datetime/pendulum 전부 올 수 있으므로
    무조건 Asia/Seoul tz-aware pendulum.DateTime 으로 변환
    """
    if dt is None:
        return pendulum.now("Asia/Seoul")

    # 문자열인 경우 (예: "2025-09-29T04:00:00+00:00" 또는 "2025-09-29 13:00:00")
    if isinstance(dt, str):
        try:
            p = pendulum.parse(dt, strict=False)
        except Exception:
            # 파싱 실패 시 현재시각 KST 반환
            return pendulum.now("Asia/Seoul")
        # tz 없으면 KST 부여, 있으면 KST로 변환
        return p.in_timezone("Asia/Seoul") if p.tzinfo else pendulum.instance(p, tz="Asia/Seoul")

    # pendulum.DateTime 인스턴스
    if isinstance(dt, pendulum.DateTime):
        return dt.in_timezone("Asia/Seoul")

    # 일반 datetime: tz 없으면 KST 부여, 있으면 KST로 변환
    if getattr(dt, "tzinfo", None) is None:
        return pendulum.instance(dt, tz="Asia/Seoul")
    return pendulum.instance(dt).in_timezone("Asia/Seoul")

@dag(
    dag_id="hourly_compact_upload",
    schedule="0 * * * *",               # 매시 정각(KST)
    start_date=START_DATE_KST,          
    catchup=True,                       # 시작시각 이후 정각들에 대해 런 생성
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["curated", "hourly", "parquet", "upbit"],
)
def hourly_compact_upload():
    @task
    def discover_files(logical_date=None):
        print(f"logical_date(raw): {logical_date!r}")

        # ✅ 항상 KST로 강제
        ld = _to_kst(logical_date)

        # 정각 정렬(필요시)
        ld = ld.replace(minute=0, second=0, microsecond=0)

        # 몇 시간을 이동할지 선택 (지금은 0시간 이동 = 현재 정각 파티션)
        # 이전 한 시간을 보려면 .subtract(hours=1) 로 바꾸면 됨.
        prev = ld.subtract(hours=0)

        # 파티션 키 계산 (KST 기준)
        dt, h = ymd(prev), hh(prev)

        prefix = staging_root(dt, h)
        print(f"[curated] discover staging prefix: s3://{S3_BUCKET}/{prefix}")

        keys = list_prefix(S3_BUCKET, prefix)
        print(f"[curated] keys({len(keys)}): {keys[:3]}{' ...' if len(keys) > 3 else ''}")

        parquet_keys = [k for k in keys if k.endswith(".parquet")]
        print(f"[curated] parquet_keys({len(parquet_keys)}): {parquet_keys[:3]}{' ...' if len(parquet_keys) > 3 else ''}")

        return {"dt": dt, "hour": h, "staging_keys": parquet_keys}

    @task
    def refine_and_write(info: dict):
        dt, h = info["dt"], info["hour"]
        if not info["staging_keys"]:
            print(f"[curated] no staging files for dt={dt}, hour={h}")
            return {"files": [], "row_count": 0, "byte_size": 0, "dt": dt, "hour": h,
                    "min_event_ts": None, "max_event_ts": None}
        print(f"[curated] found {len(info['staging_keys'])} staging files for dt={dt}, hour={h}")
        print(f"[curated] keys: {info['staging_keys']}")
        dataset = ds.dataset(
            [f"{S3_BUCKET}/{k}" for k in info["staging_keys"]],
            format="parquet",
            filesystem=pa.fs.S3FileSystem(
                region=env("AWS_DEFAULT_REGION", "ap-northeast-2")
            ),
        )
        table = dataset.to_table()
        df = table.to_pandas()

        # ---- 자연키 기준 중복 제거 ----
        if "natural_key" in df.columns:
            df = df.drop_duplicates(subset=["natural_key"], keep="last")
        else:
            key_cols = [c for c in ["market", "trade_timestamp"] if c in df.columns]
            if key_cols:
                df = df.drop_duplicates(subset=key_cols, keep="last")

        # ---- 타임스탬프 정규화 (UTC) ----
        min_event_ts = max_event_ts = None
        if "event_timestamp_ms" in df.columns:
            # 보정 생성(보유 안한 경우도 대비)
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp_ms"], unit="ms", utc=True)
        elif "trade_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["trade_timestamp"], unit="ms", utc=True)

        if "event_timestamp" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["event_timestamp"]):
                df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
            # min/max 계산 (ISO8601)
            if len(df):
                min_event_ts = df["event_timestamp"].min().isoformat()
                max_event_ts = df["event_timestamp"].max().isoformat()

        # ---- 결과 커밋 ----
        rows = df.to_dict(orient="records")
        final_prefix = curated_prefix(dt, h)
        final_key = atomic_commit_parquet(S3_BUCKET, final_prefix, rows)
        meta = head_object(S3_BUCKET, final_key) if final_key else None
        byte_size = meta["ContentLength"] if meta else 0

        print(f"[curated] committed: s3://{S3_BUCKET}/{final_key}, rows={len(df)}, bytes={byte_size}")
        return {
            "files": [final_key] if final_key else [],
            "row_count": int(len(df)),
            "byte_size": int(byte_size),
            "dt": dt, "hour": h,
            "min_event_ts": min_event_ts,
            "max_event_ts": max_event_ts,
        }

    @task
    def write_manifest(ret: dict):
        if not ret or not ret.get("files"):
            return
        m = Manifest(
            layer="curated",
            name=PROJECT_NAME,
            dt=ret["dt"],
            hour=ret["hour"],
            files=ret["files"],
            row_count=ret["row_count"],
            byte_size=ret["byte_size"],
            min_event_ts=ret.get("min_event_ts"),
            max_event_ts=ret.get("max_event_ts"),
            schema_version=SCHEMA_VERSION,
            committed_at=pendulum.now("UTC").to_iso8601_string(),
        )
        key = put_manifest(S3_BUCKET, m)
        print(f"[curated] manifest written: s3://{S3_BUCKET}/{key}")

    info = discover_files()
    cur = refine_and_write(info)
    write_manifest(cur)

hourly_compact_upload()

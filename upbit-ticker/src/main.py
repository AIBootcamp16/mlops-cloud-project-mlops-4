import os
import sys
import time
import signal
import logging
import pyarrow as pa

from config import (
    OUT_DIR,
    PROJECT,
    POLL_INTERVAL_SEC,
    FLUSH_MAX_SEC,
    MARKETS_LIMIT,
    PARQUET_COMPRESSION,
    LOG_LEVEL,
    ENABLE_HOURLY_UPLOAD,
    ENABLE_S3_UPLOAD,
    MARKETS,
)
from datetime import datetime, timezone
from parquet_appender import HourlyParquetAppender
from upbit_client import get_coin_list, get_ticker_rows
from s3_uploader import (
    merge_and_upload_previous_hour,
)

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _hour_key(dt: datetime) -> str:
    # UTC 기준 시단위 스냅샷 키 (예: 20250922T19)
    return dt.strftime("%Y%m%dT%H")


def main():
    # Arrow 스키마(필요 시 컬럼 추가는 nullable로)
    schema = pa.schema(
        [
            ("event_timestamp", pa.timestamp("ns")),
            ("market", pa.string()),
            ("trade_price", pa.float64()),
            ("change_rate", pa.float64()),
            ("acc_trade_volume", pa.float64()),
        ]
    )

    appender = HourlyParquetAppender(schema=schema)

    stop = {"flag": False}

    def _shutdown(signum, frame):
        log.warning("Signal received. Flushing & closing...")
        stop["flag"] = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info(
        f"Config → OUT_DIR={OUT_DIR}, PROJECT={PROJECT}, "
        f"POLL_INTERVAL_SEC={POLL_INTERVAL_SEC}, FLUSH_MAX_SEC={FLUSH_MAX_SEC}, "
        f"MARKETS_LIMIT={MARKETS_LIMIT}, PARQUET_COMPRESSION={PARQUET_COMPRESSION}, LOG_LEVEL={LOG_LEVEL}, "
        f"ENABLE_HOURLY_UPLOAD={ENABLE_HOURLY_UPLOAD}, "
        f"ENABLE_S3_UPLOAD={ENABLE_S3_UPLOAD}"
    )

    markets = get_coin_list() if not MARKETS else MARKETS
    log.info(f"fetch ticker for markets: {markets}")

    # 날짜/시간 경계 감지를 위한 현재 UTC 스냅샷
    current_day = datetime.now(timezone.utc).date()
    current_hour_key = _hour_key(datetime.now(timezone.utc))

    try:
        while not stop["flag"]:
            loop_start = time.time()

            # 1) 수집 및 append
            try:
                rows = get_ticker_rows(markets)
                appender.append_rows(rows)
            except Exception as e:
                log.exception(f"poll/write error: {e}")

            # 2-a) 시간 경계(정시) 감지 → '직전 시각' 병합 업로드 (옵션)
            new_hour_key = _hour_key(datetime.now(timezone.utc))
            if ENABLE_HOURLY_UPLOAD and new_hour_key != current_hour_key:
                # 직전 시각(UTC) datetime 구하기
                try:
                    prev_hour_dt = datetime.strptime(
                        current_hour_key, "%Y%m%dT%H"
                    ).replace(tzinfo=timezone.utc)
                except Exception:
                    # 파싱 실패 시 현재 시각 - 1시간으로 보정
                    prev_hour_dt = datetime.now(timezone.utc).replace(
                        minute=0, second=0, microsecond=0
                    )
                    prev_hour_dt = prev_hour_dt.replace(
                        hour=(prev_hour_dt.hour - 1) % 24
                    )

                log.info(
                    f"[HOUR] 시간 변경: {current_hour_key} → {new_hour_key}. 이전 시각 병합 업로드 시작"
                )

                try:
                    # 이전 시간 파일이 열린 상태일 수 있으니 먼저 안전하게 닫기
                    appender.close()
                except Exception as e:
                    log.exception(f"[HOUR] appender close 실패: {e}")

                try:
                    merge_and_upload_previous_hour(prev_hour_dt)
                except Exception as e:
                    log.exception(f"[HOUR] 병합 업로드 실패: {e}")

                # 다음 루프에서 새 파일이 자동 오픈됨(append 시)
                current_hour_key = new_hour_key
                log.info(f"[HOUR] 새 시간 파일 오픈 준비 완료: {current_hour_key}")

            # 3) 폴링 주기 유지
            elapsed = time.time() - loop_start
            time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))

    finally:
        appender.close()
        log.info("Graceful shutdown completed.")


if __name__ == "__main__":
    main()

import os
import time
import uuid
import logging
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

from config import (
    OUT_DIR, PROJECT, FLUSH_MAX_SEC, PARQUET_COMPRESSION
)

log = logging.getLogger(__name__)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def hour_key(dt: datetime) -> str:
    # 파일/파티션은 UTC 기준 시단위로 관리
    return dt.strftime("%Y%m%dT%H")

def make_filename(dt: datetime) -> str:
    return f"{PROJECT}_{hour_key(dt)}_{uuid.uuid4().hex}.parquet"

class HourlyParquetAppender:
    """
    - 10초 폴링으로 들어오는 row들을 버퍼에 쌓음
    - 조건: FLUSH_MAX_SEC(최대 대기 시간) 경과 또는 시간 경계 진입 시 flush
    - 중복키( market + event_timestamp ) 기반으로 같은 윈도우 내 중복 방지
    """
    def __init__(self, schema: pa.schema):
        self.out_dir = OUT_DIR
        self.schema = schema
        ensure_dir(self.out_dir)

        self.buffer = []
        self.buffer_first_ts = None
        self.last_flush_ts = time.time()

        self.current_writer = None
        self.current_file_path = None
        self.current_hour = None

        self.seen_keys = set()  # flush마다 초기화되는 윈도우 dedup 집합

    # ---------- 내부 파일 열고/닫기 ----------
    def _open_new_writer(self, when: datetime):
        self._close_writer()
        fname = make_filename(when)
        self.current_file_path = os.path.join(self.out_dir, fname)
        log.info(f"Open parquet writer: {self.current_file_path}")

        self.current_writer = pq.ParquetWriter(
            where=self.current_file_path,
            schema=self.schema,
            compression=PARQUET_COMPRESSION
        )
        self.current_hour = hour_key(when)

    def _close_writer(self):
        if self.current_writer:
            log.info(f"Close parquet writer: {self.current_file_path}")
            self.current_writer.close()
            self.current_writer = None
            self.current_file_path = None
            self.current_hour = None

    # ---------- dedup 키 ----------
    @staticmethod
    def _dedup_key(row: dict) -> str:
        raw = f"{row['market']}|{row['event_timestamp'].isoformat()}"
        return hashlib.md5(raw.encode()).hexdigest()

    # ---------- 외부 호출 ----------
    def append_rows(self, rows: list[dict]):
        if not rows:
            return

        last_ts = rows[-1]["event_timestamp"]
        if self.current_writer is None:
            self._open_new_writer(last_ts)
        elif self.current_hour != hour_key(last_ts):
            # 시간 경계 진입 → 우선 flush 후 새 파일 오픈
            self.flush()
            self._open_new_writer(last_ts)

        for r in rows:
            k = self._dedup_key(r)
            if k in self.seen_keys:
                continue
            self.seen_keys.add(k)

            if self.buffer_first_ts is None:
                self.buffer_first_ts = r["event_timestamp"]
            self.buffer.append(r)

        self._maybe_flush()

    def _maybe_flush(self):
        elapsed = time.time() - self.last_flush_ts
        if elapsed >= FLUSH_MAX_SEC:
            self.flush()

    def flush(self):
        if not self.buffer:
            self.last_flush_ts = time.time()
            return

        if self.current_writer is None:
            self._open_new_writer(self.buffer_first_ts or now_utc())

        df = pd.DataFrame(self.buffer)
        table = pa.Table.from_pandas(df, schema=self.schema, preserve_index=False)
        self.current_writer.write_table(table)

        log.info(f"Flushed {len(self.buffer)} rows to {self.current_file_path}")

        # 상태 초기화
        self.buffer.clear()
        self.buffer_first_ts = None
        self.last_flush_ts = time.time()
        self.seen_keys.clear()

    def close(self):
        self.flush()
        self._close_writer()

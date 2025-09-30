import os
import uuid
from datetime import datetime, timedelta
from dateutil import tz

KST = tz.gettz("Asia/Seoul")

def now_utc():
    return datetime.utcnow()

def hour_key(dt: datetime):
    return dt.strftime("%Y%m%dT%H")

def ymd(dt: datetime):
    return dt.strftime("%Y-%m-%d")

def hh(dt: datetime):
    return dt.strftime("%H")

def make_tmp_key(prefix: str):
    # S3 임시 커밋 경로 (_tmp 프리픽스)
    return f"{prefix}/_tmp/{uuid.uuid4().hex}.parquet"

def env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Required env var '{name}' is missing")
    return val

def join_prefix(root_prefix: str | None, path: str) -> str:
    if root_prefix:
        # 양쪽 슬래시 정리
        rp = root_prefix.strip("/")
        p  = path.lstrip("/")
        return f"{rp}/{p}"
    return path
# config.py
import os

# ---- 기본 수집/저장 설정 ----
OUT_DIR             = os.getenv("OUT_DIR", "./data")
PROJECT             = os.getenv("PROJECT", "upbit_ticker")

# 주기(초)
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", "10"))   # 10초 폴링
FLUSH_MAX_SEC       = int(os.getenv("FLUSH_MAX_SEC", "600"))      # 10분마다 강제 flush

# 수집할 KRW 마켓 개수 제한
MARKETS_LIMIT       = int(os.getenv("MARKETS_LIMIT", "5"))

# Parquet 압축 알고리즘 (zstd 권장)
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "zstd")

# 로깅 레벨
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO").upper()

# ---- S3 병합 업로드 설정 ----
# 자정에 전날 파일 병합 후 업로드를 활성화할지 여부
ENABLE_S3_UPLOAD    = os.getenv("ENABLE_S3_UPLOAD", "false").strip().lower() in (
    "1", "true", "t", "yes", "y", "on"
)

# 업로드 대상 버킷/접두사
S3_BUCKET           = os.getenv("S3_BUCKET", "your-bucket-name")
S3_PREFIX           = os.getenv("S3_PREFIX", "upbit_ticker")

# 업로드 성공 후 로컬 원본/병합 파일 삭제 여부
DELETE_AFTER_UPLOAD = os.getenv("DELETE_AFTER_UPLOAD", "true").strip().lower() in (
    "1", "true", "t", "yes", "y", "on"
)

# ---- 결측치 처리 정책 ----
# NONE_POLICY = "none" → None 저장
# NONE_POLICY = "skip" → 행 스킵
NONE_POLICY         = os.getenv("NONE_POLICY", "none").strip().lower()
if NONE_POLICY not in ("none", "skip"):
    NONE_POLICY = "none"

# ---- 시간(1h) 단위 병합/업로드 옵션 ----
ENABLE_HOURLY_UPLOAD = os.getenv("ENABLE_HOURLY_UPLOAD", "false").strip().lower() in (
    "1","true","t","yes","y","on"
)    
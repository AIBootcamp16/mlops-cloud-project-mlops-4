import os
import re
import logging
from datetime import date, datetime, timezone
from typing import List, Pattern

import boto3
import pyarrow.parquet as pq
import pyarrow as pa  # noqa: F401  # (schema 타입힌트용)

from config import (
    OUT_DIR, PROJECT, PARQUET_COMPRESSION,
    ENABLE_S3_UPLOAD, S3_BUCKET, S3_PREFIX, DELETE_AFTER_UPLOAD,
)

log = logging.getLogger(__name__)

_s3 = boto3.client("s3") if ENABLE_S3_UPLOAD else None


# -------------------------------
# 내부 유틸
# -------------------------------
def _list_files_by_pattern(pattern: Pattern) -> List[str]:
    """OUT_DIR 내에서 정규식 패턴에 맞는 파일의 절대경로 리스트를 반환."""
    files = sorted(os.listdir(OUT_DIR))
    return [os.path.join(OUT_DIR, f) for f in files if pattern.match(f)]


def _list_prevday_files(prev_day_str_compact: str) -> List[str]:
    """
    전날 파일 리스트.
    파일명 패턴: {PROJECT}_{YYYYMMDD}T{HH}_{uuid}.parquet
    prev_day_str_compact: 'YYYYMMDD'
    """
    pat = re.compile(rf"^{re.escape(PROJECT)}_{prev_day_str_compact}T\d{{2}}_.+\.parquet$")
    return _list_files_by_pattern(pat)


def _list_prevhour_files(prev_hour_compact: str) -> List[str]:
    """
    이전 '한 시간' 파일 리스트.
    파일명 패턴: {PROJECT}_{YYYYMMDD}T{HH}_{uuid}.parquet
    prev_hour_compact: 'YYYYMMDDTHH'
    """
    pat = re.compile(rf"^{re.escape(PROJECT)}_{prev_hour_compact}_.+\.parquet$")
    return _list_files_by_pattern(pat)


def _merge_parquet_files(input_paths: List[str], output_path: str):
    """
    메모리 효율을 위해 row group 단위로 읽어서 단일 ParquetWriter에 스트리밍으로 기록.
    """
    if not input_paths:
        raise ValueError("머지할 입력 파일이 없습니다.")

    # 첫 파일의 Arrow 스키마 사용
    first_pf = pq.ParquetFile(input_paths[0])
    schema = first_pf.schema_arrow

    writer = pq.ParquetWriter(
        where=output_path,
        schema=schema,
        compression=PARQUET_COMPRESSION,
    )
    try:
        for path in input_paths:
            pf = pq.ParquetFile(path)
            for rg_idx in range(pf.num_row_groups):
                table = pf.read_row_group(rg_idx)
                writer.write_table(table)
    finally:
        writer.close()


def _cleanup_local_files(paths: List[str], merged_local: str, scope_label: str):
    """업로드 성공 후 로컬 파일 정리(DELETE_AFTER_UPLOAD=true일 때)."""
    if not DELETE_AFTER_UPLOAD:
        return
    deleted = 0
    for p in paths:
        try:
            os.remove(p)
            deleted += 1
        except Exception as e:
            log.warning(f"[{scope_label}] 원본 삭제 실패: {p}: {e}")
    try:
        os.remove(merged_local)
    except Exception as e:
        log.warning(f"[{scope_label}] 병합 파일 삭제 실패: {merged_local}: {e}")
    log.info(f"[{scope_label}] 로컬 정리 완료: 원본 {deleted}/{len(paths)} 건 + 병합 파일 삭제")


# -------------------------------
# 공개 함수: 일 단위 병합 업로드
# -------------------------------
def merge_and_upload_previous_day(prev_day: date):
    """
    전날(UTC 기준) 파일들을 하나로 병합 후 S3에 업로드.
    S3 경로: s3://{bucket}/{prefix}/dt=YYYY-MM-DD/{PROJECT}_{YYYYMMDD}_merged.parquet
    """
    if not ENABLE_S3_UPLOAD or not _s3:
        log.info("ENABLE_S3_UPLOAD=false → 일 단위 S3 업로드 생략")
        return

    day_compact = prev_day.strftime("%Y%m%d")
    day_dash    = prev_day.strftime("%Y-%m-%d")

    inputs = _list_prevday_files(day_compact)
    if not inputs:
        log.info(f"[DAY] 전날 파일 없음: {prev_day} (OUT_DIR={OUT_DIR})")
        return

    merged_local = os.path.join(OUT_DIR, f"{PROJECT}_{day_compact}_merged.parquet")

    log.info(f"[DAY] {len(inputs)}개 파일 병합 → {os.path.basename(merged_local)}")
    _merge_parquet_files(inputs, merged_local)
    log.info(f"[DAY] 병합 완료: {merged_local}")

    s3_key = f"{S3_PREFIX}/dt={day_dash}/{os.path.basename(merged_local)}"
    try:
        _s3.upload_file(merged_local, S3_BUCKET, s3_key)
        log.info(f"[DAY] 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        log.exception(f"[DAY] 업로드 실패: {merged_local} → s3://{S3_BUCKET}/{s3_key}: {e}")
        # 병합 파일은 남겨둬서 재시도 가능하게 함
        return

    _cleanup_local_files(inputs, merged_local, scope_label="DAY")


# -------------------------------
# 공개 함수: 시간 단위 병합 업로드
# -------------------------------
def merge_and_upload_previous_hour(prev_hour_dt: datetime):
    """
    '이전 한 시간'의 파일들을 병합 후 S3에 업로드.
    prev_hour_dt: UTC datetime (분/초 무시)
    S3 경로: s3://{bucket}/{prefix}/dt=YYYY-MM-DD/hour=HH/{PROJECT}_{YYYYMMDDTHH}_merged.parquet
    """
    if not ENABLE_S3_UPLOAD or not _s3:
        # S3 업로드 비활성화면 조용히 종료
        return

    # 'YYYYMMDDTHH' 표기와 파티션용 'YYYY-MM-DD', 'HH'
    base_compact = prev_hour_dt.strftime("%Y%m%dT%H")
    day_dash     = prev_hour_dt.strftime("%Y-%m-%d")
    hour_str     = prev_hour_dt.strftime("%H")

    inputs = _list_prevhour_files(base_compact)
    if not inputs:
        log.info(f"[HOUR] 이전 시각 파일 없음: {base_compact}")
        return

    merged_local = os.path.join(OUT_DIR, f"{PROJECT}_{base_compact}_merged.parquet")

    log.info(f"[HOUR] {len(inputs)}개 파일 병합 → {os.path.basename(merged_local)}")
    _merge_parquet_files(inputs, merged_local)
    log.info(f"[HOUR] 병합 완료: {merged_local}")

    s3_key = f"{S3_PREFIX}/dt={day_dash}/hour={hour_str}/{os.path.basename(merged_local)}"
    try:
        _s3.upload_file(merged_local, S3_BUCKET, s3_key)
        log.info(f"[HOUR] 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        log.exception(f"[HOUR] 업로드 실패: {merged_local} → s3://{S3_BUCKET}/{s3_key}: {e}")
        return

    _cleanup_local_files(inputs, merged_local, scope_label="HOUR")

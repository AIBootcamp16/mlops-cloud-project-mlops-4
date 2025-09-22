# s3_uploader.py
import os
import re
import shutil
import logging
from datetime import date, datetime
from typing import List, Pattern

import boto3
import pyarrow.parquet as pq
import pyarrow as pa  # noqa: F401

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
    files = sorted(os.listdir(OUT_DIR))
    return [os.path.join(OUT_DIR, f) for f in files if pattern.match(f)]

def _list_prevday_files(prev_day_str_compact: str) -> List[str]:
    pat = re.compile(rf"^{re.escape(PROJECT)}_{prev_day_str_compact}T\d{{2}}_.+\.parquet$")
    return _list_files_by_pattern(pat)

def _list_prevhour_files(prev_hour_compact: str) -> List[str]:
    pat = re.compile(rf"^{re.escape(PROJECT)}_{prev_hour_compact}_.+\.parquet$")
    return _list_files_by_pattern(pat)

def _merge_parquet_files(input_paths: List[str], output_path: str):
    if not input_paths:
        raise ValueError("머지할 입력 파일이 없습니다.")
    first_pf = pq.ParquetFile(input_paths[0])
    schema = first_pf.schema_arrow
    writer = pq.ParquetWriter(output_path, schema=schema, compression=PARQUET_COMPRESSION)
    try:
        for path in input_paths:
            pf = pq.ParquetFile(path)
            for rg_idx in range(pf.num_row_groups):
                writer.write_table(pf.read_row_group(rg_idx))
    finally:
        writer.close()

def _cleanup_sources(paths: List[str], scope_label: str):
    if not DELETE_AFTER_UPLOAD:
        return
    deleted = 0
    for p in paths:
        try:
            os.remove(p)
            deleted += 1
        except Exception as e:
            log.warning(f"[{scope_label}] 원본 삭제 실패: {p}: {e}")
    log.info(f"[{scope_label}] 원본 {deleted}/{len(paths)} 건 삭제 완료")

def _save_local_as_s3_path(merged_local: str, s3_key: str, scope_label: str) -> str | None:
    """
    S3 경로(s3_key)를 OUT_DIR 아래에 동일한 디렉토리 구조로 저장.
    예: OUT_DIR/{S3_PREFIX}/dt=YYYY-MM-DD/.../filename.parquet
    """
    local_dest = os.path.join(OUT_DIR, *s3_key.split("/"))
    os.makedirs(os.path.dirname(local_dest), exist_ok=True)
    try:
        if os.path.exists(local_dest):
            os.remove(local_dest)  # 덮어쓰기
        shutil.move(merged_local, local_dest)
        log.info(f"[{scope_label}] 로컬 저장(폴백): {local_dest}")
        return local_dest
    except Exception as e:
        log.exception(f"[{scope_label}] 로컬 저장 실패: {merged_local} → {local_dest}: {e}")
        return None


# -------------------------------
# 공개 함수: 일 단위 병합 업로드
# -------------------------------
def merge_and_upload_previous_day(prev_day: date):
    """
    전날 파일 병합 → S3 업로드.
    S3 비활성/실패 시: 동일 경로 구조로 로컬 저장(폴백),
    폴백에서도 DELETE_AFTER_UPLOAD=true면 원본 조각은 삭제(병합본은 보존).
    """
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

    # 1) S3 시도
    if ENABLE_S3_UPLOAD and _s3:
        try:
            _s3.upload_file(merged_local, S3_BUCKET, s3_key)
            log.info(f"[DAY] 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
            # S3 성공 시: 원본+병합본 삭제(정책에 따름)
            if DELETE_AFTER_UPLOAD:
                _cleanup_sources(inputs, "DAY")
                try:
                    os.remove(merged_local)
                except Exception as e:
                    log.warning(f"[DAY] 병합 파일 삭제 실패: {merged_local}: {e}")
            return
        except Exception as e:
            log.exception(f"[DAY] 업로드 실패: {merged_local} → s3://{S3_BUCKET}/{s3_key}: {e}")
            # 실패 시 폴백으로 진행

    # 2) 로컬 폴백 저장(병합본 보존)
    saved = _save_local_as_s3_path(merged_local, s3_key, "DAY")
    if saved and DELETE_AFTER_UPLOAD:
        _cleanup_sources(inputs, "DAY")
        try:
            os.remove(merged_local)
        except Exception as e:
            log.warning(f"[DAY] 병합 파일 삭제 실패: {merged_local}: {e}")


# -------------------------------
# 공개 함수: 시간 단위 병합 업로드
# -------------------------------
def merge_and_upload_previous_hour(prev_hour_dt: datetime):
    """
    이전 한 시간 파일 병합 → S3 업로드.
    S3 비활성/실패 시: 동일 경로 구조로 로컬 저장(폴백),
    폴백에서도 DELETE_AFTER_UPLOAD=true면 원본 조각은 삭제(병합본은 보존).
    """
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

    # 1) S3 시도
    if ENABLE_S3_UPLOAD and _s3:
        try:
            _s3.upload_file(merged_local, S3_BUCKET, s3_key)
            log.info(f"[HOUR] 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
            if DELETE_AFTER_UPLOAD:
                _cleanup_sources(inputs, "HOUR")
                try:
                    os.remove(merged_local)
                except Exception as e:
                    log.warning(f"[HOUR] 병합 파일 삭제 실패: {merged_local}: {e}")
            return
        except Exception as e:
            log.exception(f"[HOUR] 업로드 실패: {merged_local} → s3://{S3_BUCKET}/{s3_key}: {e}")
            # 실패 시 폴백으로 진행

    # 2) 로컬 폴백 저장(병합본 보존)
    saved = _save_local_as_s3_path(merged_local, s3_key, "HOUR")
    if saved and DELETE_AFTER_UPLOAD:
        _cleanup_sources(inputs, "HOUR")
        try:
            os.remove(merged_local)
        except Exception as e:
            log.warning(f"[HOUR] 병합 파일 삭제 실패: {merged_local}: {e}")
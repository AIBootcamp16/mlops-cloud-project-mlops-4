import io
import os
import boto3
from typing import Iterable
import pyarrow as pa
import pyarrow.parquet as pq

_S3 = None

def s3():
    global _S3
    if _S3 is None:
        _S3 = boto3.client("s3")
    return _S3

def upload_bytes(bucket: str, key: str, data: bytes):
    s3().put_object(Bucket=bucket, Key=key, Body=data)

def copy_object(bucket: str, src_key: str, dst_key: str):
    s3().copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )

def delete_object(bucket: str, key: str):
    s3().delete_object(Bucket=bucket, Key=key)

def list_prefix(bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3().list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def write_parquet_bytes(rows: list[dict]) -> bytes:
    """
    매우 단순한 파이프라인용 뼈대:
    - rows(list[dict]) → Arrow Table → Parquet bytes
    - 실제 운영에서는 schema 고정, row_group_size, compression 옵션을 별도 제어하세요.
    """
    if not rows:
        return b""
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="zstd",
        use_dictionary=True,
        # row_group_size=...,  # 필요 시 지정
    )
    return buf.getvalue()

def atomic_commit_parquet(bucket: str, final_prefix: str, rows: list[dict]) -> str:
    """
    1) tmp에 parquet 업로드 → 2) 최종 위치로 copy → 3) tmp 삭제
    returns: 최종 key
    """
    from .utils import make_tmp_key
    tmp_key = make_tmp_key(final_prefix)
    data = write_parquet_bytes(rows)
    if not data:
        return ""
    upload_bytes(bucket, tmp_key, data)
    final_key = f"{final_prefix}/{os.path.basename(tmp_key)}".replace("/_tmp/", "/")
    copy_object(bucket, tmp_key, final_key)
    delete_object(bucket, tmp_key)
    return final_key

def head_object(bucket: str, key: str) -> dict | None:
    try:
        return s3().head_object(Bucket=bucket, Key=key)
    except s3().exceptions.NoSuchKey:
        return None
    except Exception:
        return None
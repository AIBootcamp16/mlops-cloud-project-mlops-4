import os, json
from dataclasses import dataclass, asdict
from .s3io import upload_bytes
from .utils import env, join_prefix

ROOT_PREFIX = env("ROOT_PREFIX", default=None)

@dataclass
class Manifest:
    layer: str
    name: str
    dt: str
    hour: str
    files: list[str]
    row_count: int
    byte_size: int
    min_event_ts: str | None
    max_event_ts: str | None
    schema_version: int
    committed_at: str

def put_manifest(bucket: str, m: Manifest):
    key_no_root = f"_manifests/{m.layer}/{m.name}/dt={m.dt}/hour={m.hour}/manifest.json"
    key = join_prefix(ROOT_PREFIX, key_no_root)
    upload_bytes(bucket, key, json.dumps(asdict(m), ensure_ascii=False).encode("utf-8"))
    return key

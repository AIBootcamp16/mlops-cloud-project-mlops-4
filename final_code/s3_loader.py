from __future__ import annotations
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.dataset as ds
import s3fs
from dotenv import load_dotenv

load_dotenv()  # .env 로드

@dataclass
class S3ParquetLoaderConfig:
    bucket: str
    base_prefix: str = "upbit"
    tz: str = "Asia/Seoul"
    columns: Optional[List[str]] = None

class S3ParquetLoader:
    def __init__(self, cfg: S3ParquetLoaderConfig):
        self.cfg = cfg
        self._tz = ZoneInfo(cfg.tz)

        self._fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "region_name": os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

    def load_today_and_yesterday(self, ref_dt: Optional[datetime] = None) -> pd.DataFrame:
        now = ref_dt.astimezone(self._tz) if ref_dt else datetime.now(self._tz)
        today = now.date()
        yesterday = today - timedelta(days=1)

        date_folders = [today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")]

        uris: List[str] = []
        for d in date_folders:
            prefix = f"{self.cfg.base_prefix}/{d}"
            print(f"s3://{self.cfg.bucket}/{prefix}/*.parquet")
            files = self._fs.glob(f"s3://{self.cfg.bucket}/{prefix}/*.parquet")
            uris.extend(files)

        if not uris:
            return pd.DataFrame()

        dataset = ds.dataset(uris, format="parquet", filesystem=self._fs)
        table = dataset.to_table(columns=self.cfg.columns) if self.cfg.columns else dataset.to_table()
        return table.to_pandas().reset_index(drop=True)

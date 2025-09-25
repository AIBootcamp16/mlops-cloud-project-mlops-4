from s3_loader import S3ParquetLoader, S3ParquetLoaderConfig
import pandas as pd

cfg = S3ParquetLoaderConfig(
    bucket="raw-data-bucket-moasic-mlops-4",
    base_prefix="upbit_ticker",
)

loader = S3ParquetLoader(cfg)
df = loader.load_today_and_yesterday()
# 밀리초 epoch → UTC datetime
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
# UTC → Asia/Seoul 변환
df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Seoul")
# 타임존 제거 (naive datetime으로 변환)
df["timestamp"] = df["timestamp"].dt.tz_localize(None)
print(df.shape)
print(df.head())

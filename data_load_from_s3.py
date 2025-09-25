import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "raw-data-bucket-moasic-mlops-4"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
start_utc = now_utc - timedelta(hours=24)
date_list = pd.date_range(start=start_utc.date(), end=now_utc.date()).strftime("%Y-%m-%d").tolist()

dfs = []

for date in date_list:
    prefix = f"upbit_ticker/{date}/"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if "Contents" not in response:
        continue

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):
            continue
        
        file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        df_temp = pd.read_parquet(BytesIO(file_obj['Body'].read()))
        df_temp['event_timestamp'] = pd.to_datetime(df_temp['timestamp'], unit='ms') + pd.Timedelta(hours=9)

        start_kst = start_utc + timedelta(hours=9)
        end_kst = now_utc + timedelta(hours=9)
        df_temp = df_temp[(df_temp['event_timestamp'] >= start_kst) & (df_temp['event_timestamp'] <= end_kst)]
        
        if not df_temp.empty:
            dfs.append(df_temp)
            print(f"Loaded (24h range): {key}")

if len(dfs) == 0:
    print("24시간 범위 내 parquet 파일이 없습니다.")
    df = pd.DataFrame()
else:
    df = pd.concat(dfs, ignore_index=True) 
    print("All Parquet files concatenated. Shape:", df.shape)

if not df.empty:
    # 시 단위로 내림 (정시 기준)
    df['event_hour'] = df['event_timestamp'].dt.floor('H')

    coins = ['KRW-BTC', 'KRW-DOGE', 'KRW-ETH', 'KRW-XRP']
    coin_dfs = {}

    for coin in coins:
        # 각 정시별 첫 거래 선택
        coin_df = df[df['market'] == coin].groupby('event_hour').first().reset_index()
        coin_dfs[coin] = coin_df
        print(f"{coin} shape:", coin_df.shape)
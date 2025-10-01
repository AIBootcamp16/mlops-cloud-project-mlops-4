import os
import json
import numpy as np
import pandas as pd
import mlflow
import mlflow.keras
from joblib import dump
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from datetime import datetime, timedelta
from io import BytesIO
import boto3
from dotenv import load_dotenv

# mlflow_model_manager.py에서 클래스 임포트
from mlflow_model_manager import BestModelSelector, ModelRegistryManager

# -------------------------------
# 0️⃣ MLflow 서버 URI 설정
# -------------------------------
remote_server_uri = "http://3.39.10.103:5000"
mlflow.set_tracking_uri(remote_server_uri)
mlflow.set_experiment("bitcoin-lstm_2")
mlflow.autolog()

# -------------------------------
# AWS 환경변수 설정
# -------------------------------
load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME]):
    print("AWS 환경변수가 설정되지 않았습니다. 더미 데이터를 사용합니다.")
    USE_AWS = False
else:
    USE_AWS = True
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

# =========================================
# 1️⃣ 데이터 로드 및 전처리 (수정된 부분)
# =========================================
def load_training_data(bucket_name: str, csv_key: str, coin: str):
    """
    CSV 기반 과거 학습 데이터 + S3 Parquet 전체 데이터 통합
    """
    # 1️⃣ CSV 로드
    if USE_AWS:
        try:
            csv_obj = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
            df_csv = pd.read_csv(csv_obj['Body'], parse_dates=['timestamp'])
            df_csv = df_csv.sort_values('timestamp').drop_duplicates('timestamp')
            df_csv['event_timestamp'] = pd.to_datetime(df_csv['timestamp'])
            df_csv['event_hour'] = df_csv['event_timestamp'].dt.floor('H')
        except Exception as e:
            print(f"CSV 로드 실패: {e}")
            df_csv = pd.DataFrame()
    else:
        df_csv = pd.DataFrame()

    # 2️⃣ S3 Parquet 전체 로드
    dfs = []
    if USE_AWS:
        start_date = datetime(2025,9,24).date()  # CSV 종료 다음날+1일(9/24 부터 timestamp 컬럼 기준)
        end_date = datetime.utcnow().date()
        date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()

        for date in date_list:
            prefix = f"upbit_ticker/{date}/"
            try:
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            except Exception as e:
                print(f"S3 리스트 조회 실패: {e}")
                continue

            if "Contents" not in response:
                continue

            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                try:
                    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                    df_temp = pd.read_parquet(BytesIO(file_obj['Body'].read()))
                    df_temp['event_timestamp'] = pd.to_datetime(df_temp['timestamp'], unit='ms') + pd.Timedelta(hours=9)
                    df_temp = df_temp[df_temp['event_timestamp'] >= datetime(2025,9,23,17) + timedelta(hours=9)]

                    if not df_temp.empty:
                        dfs.append(df_temp)
                        # print(f"Loaded Parquet: {key}")
                except Exception as e:
                    print(f"Parquet 로드 실패 {key}: {e}")
                    continue

    df_parquet_coin = pd.DataFrame()
    if len(dfs) > 0:
        df_parquet = pd.concat(dfs, ignore_index=True)
        df_parquet['event_hour'] = df_parquet['event_timestamp'].dt.floor('H')
        df_parquet_coin = df_parquet[df_parquet['market'] == coin].groupby('event_hour').first().reset_index()

    # 3️⃣ CSV + Parquet 병합
    if not df_csv.empty:
        df_csv_coin = df_csv[df_csv['market'] == coin] if 'market' in df_csv.columns else df_csv.copy()
        df_combined = pd.concat([df_csv_coin, df_parquet_coin], ignore_index=True)
    else:
        df_combined = df_parquet_coin

    if not df_combined.empty:
        df_combined = df_combined.drop_duplicates(subset=['event_hour'], keep='last')
        df_combined = df_combined.sort_values('event_hour').reset_index(drop=True)

    return df_combined

# -------------------------------
# 2️⃣ 학습용 데이터 로드
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_key = "training data/KRW-BTC_historical.csv"
coin = "KRW-BTC"

df = load_training_data(BUCKET_NAME, csv_key, coin)
print(df.shape)

# features 배열 생성 (기존 컬럼만 사용)
features = df[['trade_price','acc_trade_volume']].copy()

# 이동평균 & lag 컬럼 추가
features['MA_3'] = features['trade_price'].rolling(3).mean()
features['MA_6'] = features['trade_price'].rolling(6).mean()
features['lag_1'] = features['trade_price'].shift(1)

# 결측 제거
features = features.dropna().values
print("2:", features.shape)

sequence_length = 24
def create_sequences(data, seq_len):
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len, 0])
    return np.array(X), np.array(y)

X, y = create_sequences(features, sequence_length)

train_size = int(len(X) * 0.7)
val_size = int(len(X) * 0.2)
X_train, y_train = X[:train_size], y[:train_size]
X_val, y_val = X[train_size:train_size+val_size], y[train_size:train_size+val_size]
X_test, y_test = X[train_size+val_size:], y[train_size+val_size:]

num_features = X_train.shape[2]
scaler_X = MinMaxScaler()
X_train_scaled = scaler_X.fit_transform(X_train.reshape(-1, num_features)).reshape(X_train.shape)
X_val_scaled = scaler_X.transform(X_val.reshape(-1, num_features)).reshape(X_val.shape)
X_test_scaled = scaler_X.transform(X_test.reshape(-1, num_features)).reshape(X_test.shape)

scaler_y = MinMaxScaler()
y_train_scaled = scaler_y.fit_transform(y_train.reshape(-1, 1))
y_val_scaled = scaler_y.transform(y_val.reshape(-1, 1))
y_test_scaled = scaler_y.transform(y_test.reshape(-1, 1))

# ==============================
# 3. MLflow Run 시작
# ==============================
with mlflow.start_run(run_name="LSTM_24h")  :

    ARTIFACT_DIR = "artifacts"
    
    # ------------------------------
    # 데이터 정보 로깅 (추가)
    # ------------------------------
    if not df.empty:
        num_rows, num_columns = features.shape
        start_date = df['event_timestamp'].min()
        end_date = df['event_timestamp'].max()

        mlflow.log_param("num_rows", num_rows)
        mlflow.log_param("num_columns", num_columns)
        mlflow.log_param("start_date", start_date)
        mlflow.log_param("end_date", end_date)

        # tail 5개 artifact로 기록
        tail_path = os.path.join(ARTIFACT_DIR, "tail_5.csv")
        os.makedirs(ARTIFACT_DIR, exist_ok=True)
        df.tail(5).to_csv(tail_path, index=False)
        mlflow.log_artifact(tail_path)

    # ------------------------------
    # 하이퍼파라미터 로깅
    # ------------------------------
    hidden_units = 64
    epochs = 50
    batch_size = 32
    mlflow.log_param("sequence_length", sequence_length)
    mlflow.log_param("hidden_units", hidden_units)
    mlflow.log_param("epochs", epochs)
    mlflow.log_param("batch_size", batch_size)
    mlflow.log_param("num_features", num_features)

    # ------------------------------
    # 모델 정의 & 학습
    # ------------------------------
    model = Sequential([
        LSTM(hidden_units, input_shape=(X_train_scaled.shape[1], X_train_scaled.shape[2])),
        Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')
    model.fit(
        X_train_scaled, y_train_scaled,
        validation_data=(X_val_scaled, y_val_scaled),
        epochs=epochs,
        batch_size=batch_size,
        verbose=0
    )

    # ------------------------------
    # 모델 평가
    # ------------------------------
    y_pred = model.predict(X_test_scaled)
    rmse = np.sqrt(mean_squared_error(y_test_scaled, y_pred))
    mae = mean_absolute_error(y_test_scaled, y_pred)

    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)

    # ------------------------------
    # Artifact 저장
    # ------------------------------
    os.makedirs(ARTIFACT_DIR, exist_ok=True)

    # 모델 저장
    model_path = os.path.join(ARTIFACT_DIR, "lstm_model.keras")
    model.save(model_path)
    mlflow.log_artifact(model_path)

    # 스케일러 저장
    scaler_X_path = os.path.join(ARTIFACT_DIR, "scaler_X.joblib")
    scaler_y_path = os.path.join(ARTIFACT_DIR, "scaler_y.joblib")
    dump(scaler_X, scaler_X_path)
    dump(scaler_y, scaler_y_path)
    mlflow.log_artifact(scaler_X_path)
    mlflow.log_artifact(scaler_y_path)

    # 피처 리스트 저장
    feature_list = ['trade_price','acc_trade_volume','MA_3','MA_6','lag_1']
    feature_path = os.path.join(ARTIFACT_DIR, "features.json")
    with open(feature_path, "w") as f:
        json.dump(feature_list, f)
    mlflow.log_artifact(feature_path)

    # ------------------------------
    # 태그 설정
    # ------------------------------
    mlflow.set_tag("model_type", "LSTM")
    mlflow.set_tag("developer", "yein")

    # ------------------------------
    # Keras 모델 MLflow 로깅
    # ------------------------------
    mlflow.keras.log_model(model, "lstm-model")


    # ------------------------------
    # 최적 모델 선택 + 모델 Registry 등록 + 태그 업데이트
    # ------------------------------

    # 1. 최적 모델 선택
    selector = BestModelSelector(experiment_name="bitcoin-lstm_2", metric_name="rmse")
    best_run = selector.get_best_run()

    # 2. 최적 모델 등록 / champion 선정
    registry = ModelRegistryManager(model_name="MyLSTMModel_bitcoin_2")
    registry.register_model(
        run_id=best_run.info.run_id,
        artifact_path="lstm-model",
        tags={"model_type":"LSTM","developer":"yein","dataset":"2years","market":"bitcoin"}
    )   
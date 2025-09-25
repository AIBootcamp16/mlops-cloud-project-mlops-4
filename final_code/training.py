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
from s3_loader import S3ParquetLoader, S3ParquetLoaderConfig
from data_preprocessor import DataPreprocessor

# ==============================
# 0. MLflow 서버 URI 설정 (필요하면)
# ==============================
remote_server_uri = "http://3.39.10.103:5000"
mlflow.set_tracking_uri(remote_server_uri)

# mlflow.set_tracking_uri("file:///tmp/mlruns") #로컬 테스트용

# ==============================
# 1. 실험 설정
# ==============================
mlflow.set_experiment("bitcoin-lstm")

# mlflow.tensorflow.autolog()
# ==============================    
# 2. 데이터 로드 & 전처리
# ==============================
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# DATA_PATH = os.path.join(BASE_DIR, "data", "KRW-BTC_historical.csv")
# df = pd.read_csv(DATA_PATH, parse_dates=['timestamp'])
# files = ["KRW-BTC_historical.csv"]
# for f in files:
#     df = pd.read_csv(f"data/{f}", parse_dates=['timestamp'])
#     df = df.sort_values('timestamp')

print("2. data loading")

cfg = S3ParquetLoaderConfig(
    bucket="raw-data-bucket-moasic-mlops-4",
    base_prefix="upbit_ticker",
)

loader = S3ParquetLoader(cfg)
df = loader.load_today_and_yesterday()

print("3. data preprocessing")

dt = DataPreprocessor()
df = dt.market_filter(df, "KRW-BTC")
df = dt.time_transform(df, "Asia/Seoul")

# timestamp 기준 중복 제거
#TODO: 실제로 필요한지 확인, 현재 중복되면 마지막 값으로 남김
df = dt.deduplicate(df, how="last")

df = df.set_index('timestamp').resample('1h').ffill()

df['MA_3'] = df['trade_price'].rolling(3).mean()
df['MA_6'] = df['trade_price'].rolling(6).mean()
df['lag_1'] = df['trade_price'].shift(1)
df = df.dropna()

sequence_length = 24
def create_sequences(data, seq_len):
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len, 0])
    return np.array(X), np.array(y)

features = df[['trade_price','acc_trade_volume','MA_3','MA_6','lag_1']].values
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
with mlflow.start_run(run_name="LSTM_24h_s3_loader")  :

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
    ARTIFACT_DIR = "artifacts"
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

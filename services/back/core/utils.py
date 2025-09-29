import numpy as np
import pandas as pd
import asyncio
import mlflow
from mlflow.tracking import MlflowClient
import mlflow.keras
import joblib
from core.config import settings

mlflow.set_tracking_uri(settings.ML_FLOW_URL)
client = MlflowClient()

async def scale_model_load():
    model_name = settings.MODEL_NAME
    version_alias = settings.MODEL_VERSION_ALIAS

    model_info = await asyncio.to_thread(
        client.get_model_version_by_alias,
        model_name,
        version_alias
    )
    run_id = model_info.run_id

    model_uri = f"models:/{model_name}@{version_alias}"
    model = await asyncio.to_thread(mlflow.keras.load_model, model_uri)

    scaler_X_path = await asyncio.to_thread(
        mlflow.artifacts.download_artifacts,
        run_id=run_id,
        artifact_path="scaler_X.joblib"
    )
    scaler_X = await asyncio.to_thread(joblib.load, scaler_X_path)

    scaler_y_path = await asyncio.to_thread(
        mlflow.artifacts.download_artifacts,
        run_id=run_id,
        artifact_path="scaler_y.joblib"
    )
    scaler_y = await asyncio.to_thread(joblib.load, scaler_y_path)

    print("✅ Model and scalers loaded successfully!")
    return model, scaler_X, scaler_y

def data_preprocess(df: pd.DataFrame):
    df_btc = df.copy()
    print(df.shape)
    df_btc["timestamp"] = pd.to_datetime(df_btc["event_timestamp"])
    df_btc = df_btc.set_index("timestamp")
    df_btc = df_btc.resample("1H").ffill()
    df_btc["MA_3"] = df_btc["trade_price"].rolling(3).mean()
    df_btc["MA_6"] = df_btc["trade_price"].rolling(6).mean()
    df_btc["lag_1"] = df_btc["trade_price"].shift(1)
    
    df_btc = df_btc.dropna()
    features = df_btc[["trade_price", "acc_trade_volume", "MA_3", "MA_6", "lag_1"]].values

    def create_sequences(data, seq_len):
        print(seq_len)
        X = []
        for i in range(len(data) - seq_len + 1):
            X.append(data[i:i+seq_len])
        return np.array(X)

    seq_len = 24
    X_input = create_sequences(features, seq_len)
    if X_input.shape[0] == 0:
        raise ValueError(f"데이터가 부족합니다. 최소 {seq_len}행 이상 필요, 현재 데이터 {len(features)}행////{df.shape}")

    num_features = features.shape[1]
    return X_input, num_features

async def model_predict(model, X_input, scaler_X, scaler_y, num_features):
    X_input_scaled = scaler_X.transform(X_input.reshape(-1, num_features)).reshape(X_input.shape)
    y_pred_scaled = await asyncio.to_thread(model.predict, X_input_scaled)
    y_pred = scaler_y.inverse_transform(y_pred_scaled)
    print("다음 1시간 예측 가격: {:,.1f} 원".format(y_pred[0][0]))
    return y_pred

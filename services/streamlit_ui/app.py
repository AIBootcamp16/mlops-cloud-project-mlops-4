import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import boto3
from io import BytesIO
import os
from dotenv import load_dotenv
import httpx
import asyncio

load_dotenv()

# -------------------------
# AWS 환경변수
# -------------------------
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# AWS 자격 증명 체크
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME]):
    st.warning("AWS 환경변수가 설정되지 않았습니다. 더미 데이터를 사용합니다.")
    USE_AWS = False
else:
    USE_AWS = True
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

# -------------------------
# S3 데이터 로드 함수
# -------------------------
def load_s3_parquet_hours(bucket_name: str, coins: list, hours: int = 30):
    if not USE_AWS:
        return {coin: pd.DataFrame() for coin in coins}
    
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_utc = now_utc - timedelta(hours=hours)
    date_list = pd.date_range(start=start_utc.date(), end=now_utc.date()).strftime("%Y-%m-%d").tolist()
    dfs = []

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

                start_kst = start_utc + timedelta(hours=9)
                end_kst = now_utc + timedelta(hours=9)
                df_temp = df_temp[(df_temp['event_timestamp'] >= start_kst) & (df_temp['event_timestamp'] <= end_kst)]

                if not df_temp.empty:
                    dfs.append(df_temp)
                    print(f"Loaded ({hours}h range): {key}")
            except Exception as e:
                print(f"파일 로드 실패 {key}: {e}")
                continue

    if len(dfs) == 0:
        return {coin: pd.DataFrame() for coin in coins}

    df = pd.concat(dfs, ignore_index=True)
    df['event_hour'] = df['event_timestamp'].dt.floor('H')

    coin_dfs = {}
    for coin in coins:
        coin_df = df[df['market'] == coin].groupby('event_hour').first().reset_index()
        coin_dfs[coin] = coin_df

    return coin_dfs

def load_s3_parquet_incremental(bucket_name: str, coins: list, existing_data: dict, last_hour: datetime):
    """증분 데이터만 로드하는 함수"""
    if not USE_AWS:
        return existing_data
    
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    
    # 마지막 로드 시각과 현재 시각의 차이 계산
    last_hour_utc = last_hour.replace(tzinfo=None) - timedelta(hours=9)  # KST to UTC
    hours_diff = int((now_utc - last_hour_utc).total_seconds() / 3600)
    
    if hours_diff <= 0:
        print("증분 데이터 없음 - 기존 데이터 유지")
        return existing_data
    
    print(f"증분 데이터 로드: 최근 {hours_diff}시간")
    
    # 증분 데이터만 로드
    start_utc = now_utc - timedelta(hours=hours_diff)
    date_list = pd.date_range(start=start_utc.date(), end=now_utc.date()).strftime("%Y-%m-%d").tolist()
    dfs = []

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

                start_kst = start_utc + timedelta(hours=9)
                end_kst = now_utc + timedelta(hours=9)
                df_temp = df_temp[(df_temp['event_timestamp'] >= start_kst) & (df_temp['event_timestamp'] <= end_kst)]

                if not df_temp.empty:
                    dfs.append(df_temp)
                    print(f"Incremental load: {key}")
            except Exception as e:
                print(f"파일 로드 실패 {key}: {e}")
                continue

    if len(dfs) == 0:
        print("증분 데이터 없음")
        return existing_data

    df_new = pd.concat(dfs, ignore_index=True)
    df_new['event_hour'] = df_new['event_timestamp'].dt.floor('H')

    # 기존 데이터와 병합
    updated_coin_dfs = {}
    for coin in coins:
        new_coin_df = df_new[df_new['market'] == coin].groupby('event_hour').first().reset_index()
        
        if coin in existing_data and not existing_data[coin].empty:
            # 기존 데이터와 합치고 최근 30시간만 유지
            combined = pd.concat([existing_data[coin], new_coin_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['event_hour'], keep='last')
            combined = combined.sort_values('event_hour').tail(30).reset_index(drop=True)
            updated_coin_dfs[coin] = combined
        else:
            updated_coin_dfs[coin] = new_coin_df

    return updated_coin_dfs

# -------------------------
# 예측 API 호출 함수들
# -------------------------
async def fetch_prediction_async(coin: str, model: str, df: pd.DataFrame):
    """비동기 예측 API 호출"""
    url = "http://localhost:8000/predict/lstm"
    
    # DataFrame을 dict로 변환 (최근 30시간 데이터 전송)
    df_clean = df.tail(30).copy()
    
    # Timestamp를 문자열로 변환하여 JSON 직렬화 가능하게 만들기
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df_payload = df_clean.to_dict(orient="records")

    payload = {
        "coin": coin,
        "model": model,
        "recent_data": df_payload
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=10.0)
            response.raise_for_status()
            return response.json()
    except httpx.ConnectError:
        return {"error": "connection_failed", "message": "예측 서버에 연결할 수 없습니다"}
    except httpx.TimeoutException:
        return {"error": "timeout", "message": "예측 API 응답 시간 초과"}
    except Exception as e:
        return {"error": "unknown", "message": f"예측 API 호출 실패: {e}"}

def fetch_prediction(coin: str, model: str, df: pd.DataFrame):
    """동기 래퍼 함수"""
    try:
        return asyncio.run(fetch_prediction_async(coin, model, df))
    except Exception as e:
        return {"error": "asyncio_failed", "message": f"비동기 호출 실패: {e}"}

def generate_dummy_prediction(current_price: float, volatility: float, model: str):
    """더미 예측값 생성"""
    # 모델별 특성 반영
    model_factors = {
        "LSTM": {"trend": 0.02, "noise": 0.8},
        "GRU": {"trend": 0.015, "noise": 0.75},
        "Prophet": {"trend": 0.01, "noise": 0.9},
        "LightGBM": {"trend": 0.025, "noise": 0.7}
    }
    
    factor = model_factors.get(model, {"trend": 0.02, "noise": 0.8})
    
    # 예측 가격 (약간의 상승 바이어스 + 랜덤 노이즈)
    trend = np.random.normal(factor["trend"], volatility * 0.5)
    pred_price = current_price * (1 + trend)
    
    # 신뢰도 (모델별로 다르게)
    base_confidence = factor["noise"]
    confidence = base_confidence + np.random.uniform(-0.1, 0.1)
    confidence = max(0.6, min(0.95, confidence))
    
    return {
        'predicted_price': pred_price,
        'confidence': confidence,
        'model_used': model,
        'metrics': {
            'rmse': np.random.uniform(15000, 45000),
            'mae': np.random.uniform(8000, 25000),
            'r2': np.random.uniform(0.75, 0.92)
        }
    }

def create_dummy_data(coin: str, period: int = 30):
    """더미 데이터 생성 함수"""
    now = datetime.now()
    timestamps = [now - timedelta(hours=i) for i in range(period)][::-1]
    
    # 코인별 기본 가격 설정
    base_prices = {
        'KRW-BTC': 73000000,
        'KRW-ETH': 3500000,
        'KRW-DOGE': 180,
        'KRW-XRP': 700
    }
    
    base_price = base_prices.get(coin, 50000)
    # 좀 더 현실적인 가격 변동
    price_changes = np.random.randn(period) * base_price * 0.005  # 0.5% 변동성
    prices = base_price + np.cumsum(price_changes)
    volumes = np.random.uniform(50, 500, period)  # 거래량
    
    return pd.DataFrame({
        "event_hour": timestamps,
        "trade_price": prices,
        "trade_volume": volumes
    })

# -------------------------
# Streamlit 앱 구성
# -------------------------
st.set_page_config(page_title="Bitcoin 예측 사이트", layout="wide")

# 세션 스테이트 초기화 (데이터 및 마지막 로드 시간 저장)
if 'coin_data' not in st.session_state:
    st.session_state.coin_data = None
    st.session_state.last_load_hour = None

# 사이드바 설정
st.sidebar.header("⚙️ 설정")
coins = ['KRW-BTC', 'KRW-ETH', 'KRW-DOGE', 'KRW-XRP']
selected_coin = st.sidebar.selectbox("코인 선택", coins)
data_load_hours = 30  # 데이터 로드 시간
display_hours = 24     # 시각화 시간
model = st.sidebar.selectbox("예측 모델 선택", ["LSTM", "GRU", "Prophet", "LightGBM"])

# -------------------------
# 캐싱된 데이터 로드
# -------------------------
@st.cache_data(ttl=3600)
def load_coin_data_initial(coin_list, hours):
    """초기 전체 데이터 로드"""
    return load_s3_parquet_hours(BUCKET_NAME, coin_list, hours=hours)

def load_coin_data_smart(coin_list, hours):
    """스마트 데이터 로드: 초기에는 전체, 이후에는 증분만"""
    current_hour_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    
    # 첫 로드이거나 세션 데이터가 없는 경우
    if st.session_state.coin_data is None or st.session_state.last_load_hour is None:
        print("초기 데이터 로드 (전체 30시간)")
        coin_dfs = load_coin_data_initial(coin_list, hours)
        st.session_state.coin_data = coin_dfs
        st.session_state.last_load_hour = current_hour_dt
        return coin_dfs
    
    # 시간이 바뀌지 않았으면 기존 데이터 반환
    if st.session_state.last_load_hour == current_hour_dt:
        print("캐시된 데이터 사용")
        return st.session_state.coin_data
    
    # 시간이 바뀌었으면 증분 데이터만 로드
    print(f"증분 데이터 로드 시작: {st.session_state.last_load_hour} -> {current_hour_dt}")
    updated_data = load_s3_parquet_incremental(
        BUCKET_NAME, 
        coin_list, 
        st.session_state.coin_data, 
        st.session_state.last_load_hour
    )
    st.session_state.coin_data = updated_data
    st.session_state.last_load_hour = current_hour_dt
    return updated_data

# 제목과 버튼
col_title, col_button = st.columns([8, 2])
with col_title:
    st.title("🚀 Bitcoin 1시간 단위 가격 예측")
with col_button:
    predict_btn = st.button("🔄 새로고침", help="최신 예측값으로 업데이트")

# -------------------------
# 데이터 로드
# -------------------------
with st.spinner("데이터를 불러오는 중..."):
    try:
        coin_dfs = load_coin_data_smart(coins, data_load_hours)
        if USE_AWS:
            if st.session_state.last_load_hour:
                load_type = "증분" if st.session_state.last_load_hour < datetime.now().replace(minute=0, second=0, microsecond=0) else "캐시"
                st.success(f"S3 데이터 로드 완료! ({load_type})")
            else:
                st.success("S3 데이터 로드 완료!")
        else:
            st.info("더미 데이터를 사용합니다.")
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        coin_dfs = {coin: pd.DataFrame() for coin in coins}

# -------------------------
# 선택 코인 데이터 처리
# -------------------------
df = coin_dfs.get(selected_coin, pd.DataFrame())

# 데이터가 없으면 더미 생성
if df.empty:
    st.warning("선택한 코인의 30시간 데이터가 없습니다. 더미 데이터를 사용합니다.")
    df = create_dummy_data(selected_coin, data_load_hours)

# 데이터가 충분하지 않은 경우 체크
if len(df) < 2:
    st.error("데이터가 부족합니다.")
    st.stop()

# -------------------------
# 예측 실행
# -------------------------
@st.cache_data(ttl=300)  # 5분간 캐시
def get_cached_prediction(coin: str, model: str, data_hash: str, current_hour: str):
    """캐시된 예측 결과 - 현재 시각(시간)이 바뀌면 자동으로 새로 요청"""
    # 전역 df 변수를 사용하여 예측 API 호출
    result = fetch_prediction(coin, model, df)
    return result

# 현재 시각(시간 단위)을 캐싱 키에 포함
current_hour = datetime.now().strftime("%Y-%m-%d %H:00:00")

# 데이터 해시 생성 (캐싱 키)
data_hash = hash(str(df['trade_price'].values.tobytes()) + selected_coin + model)

# 예측 상태 표시
prediction_status = st.empty()

# 새로고침 버튼이 눌렸을 때 캐시 클리어 및 증분 데이터 로드
if predict_btn:
    get_cached_prediction.clear()
    # 현재 시간으로 강제 업데이트 트리거
    current_hour_for_refresh = datetime.now().replace(minute=0, second=0, microsecond=0)
    if st.session_state.last_load_hour and st.session_state.last_load_hour < current_hour_for_refresh:
        st.session_state.last_load_hour = None  # 증분 로드 강제 실행
    st.rerun()

# 예측 실행
with st.spinner("🔮 예측 중..."):
    try:
        prediction_result = get_cached_prediction(selected_coin, model, str(data_hash), current_hour)
        
        # 예측 결과 처리
        if prediction_result and 'predicted_price' in prediction_result:
            prediction_status.success("✅ AI 모델 예측 완료!")
            prediction_source = "🤖 AI 모델"
            use_ai_prediction = True
        elif prediction_result and 'error' in prediction_result:
            # 에러 메시지 표시
            error_msg = prediction_result.get('message', '알 수 없는 오류')
            prediction_status.warning(f"🔄 {error_msg} - 통계 기반 예측 사용")
            
            # 통계적 예측 생성
            current_price = df["trade_price"].iloc[-1]
            volatility = df["trade_price"].pct_change().std()
            prediction_result = generate_dummy_prediction(current_price, volatility, model)
            prediction_source = "📊 통계 모델"
            use_ai_prediction = False
        else:
            prediction_status.info("🔄 예측 서버 응답 없음 - 통계 기반 예측 사용")
            current_price = df["trade_price"].iloc[-1]
            volatility = df["trade_price"].pct_change().std()
            prediction_result = generate_dummy_prediction(current_price, volatility, model)
            prediction_source = "📊 통계 모델"
            use_ai_prediction = False
            
    except Exception as e:
        st.error(f"예측 시스템 오류: {e}")
        current_price = df["trade_price"].iloc[-1]
        volatility = df["trade_price"].pct_change().std() if len(df) > 1 else 0.02
        prediction_result = generate_dummy_prediction(current_price, volatility, model)
        prediction_status.error("❌ 예측 시스템 오류 - 기본값 사용")
        prediction_source = "🔧 기본 모델"
        use_ai_prediction = False

# -------------------------
# 예측값 계산
# -------------------------
pred_price = prediction_result['predicted_price']
prediction_confidence = prediction_result.get('confidence', 0.85)
current_price = df["trade_price"].iloc[-1]
pred_change = (pred_price - current_price) / current_price * 100

# -------------------------
# 메트릭 카드
# -------------------------
col1, col2, col3 = st.columns(3)

prev_price = df['trade_price'].iloc[-2] if len(df) >= 2 else current_price
price_change = (current_price - prev_price) / prev_price * 100 if prev_price != 0 else 0

col1.metric(
    "현재 가격",
    f"{current_price:,.0f}",
    f"{price_change:.2f}%"
)
col2.metric("24h 거래량", f"{df['trade_volume'].sum():,.5f}")

# 예측 방향에 따른 색상 표시
pred_color = "normal"
if abs(pred_change) > 2:
    pred_color = "inverse" if pred_change < 0 else "normal"

col3.metric(
    "1시간 뒤 예측", 
    f"{pred_price:,.0f}", 
    f"{pred_change:.2f}%",
    delta_color=pred_color,
    help=f"{prediction_source} (신뢰도: {prediction_confidence:.1%})"
)

# -------------------------
# 예측 신뢰도 표시
# -------------------------
confidence_col1, confidence_col2 = st.columns([3, 1])
with confidence_col1:
    st.progress(prediction_confidence, text=f"예측 신뢰도: {prediction_confidence:.1%}")
with confidence_col2:
    if use_ai_prediction:
        st.success("🟢 AI 활성")
    else:
        st.warning("🟡 통계 모드")

# -------------------------
# Plotly 차트 (24시간 데이터만 시각화)
# -------------------------
# 시각화용으로 최근 24시간 데이터만 추출
df_display_chart = df.tail(display_hours).copy()

fig = go.Figure()

# 실제 가격 라인
fig.add_trace(go.Scatter(
    x=df_display_chart["event_hour"],
    y=df_display_chart["trade_price"],
    mode="lines+markers",
    name="실제 가격",
    line=dict(color="blue", width=2),
    marker=dict(size=4)
))

# 예측 시점 추가
next_hour = df_display_chart["event_hour"].iloc[-1] + timedelta(hours=1)
pred_color_line = "red" if pred_change > 0 else "orange"

fig.add_trace(go.Scatter(
    x=[df_display_chart["event_hour"].iloc[-1], next_hour],
    y=[current_price, pred_price],
    mode="lines+markers+text",
    name="예측 가격",
    line=dict(color=pred_color_line, dash="dash", width=3),
    marker=dict(color=pred_color_line, size=8),
    text=[None, f"{pred_price:,.0f}원"],
    textposition="top center",
    textfont=dict(size=12, color=pred_color_line)
))

# 신뢰구간 추가 (예측값 기준 ±5%)
confidence_band = pred_price * 0.05
fig.add_trace(go.Scatter(
    x=[next_hour, next_hour, next_hour],
    y=[pred_price - confidence_band, pred_price, pred_price + confidence_band],
    mode="markers",
    name="신뢰구간",
    marker=dict(color=pred_color_line, size=3, opacity=0.3),
    showlegend=False
))

fig.update_layout(
    title=f"{selected_coin} 최근 {display_hours}시간 가격 추이 & 예측",
    xaxis_title="시간",
    yaxis_title="가격 (KRW)",
    template="plotly_white",
    height=500,
    hovermode='x unified'
)

st.plotly_chart(fig, use_container_width=True)

# -------------------------
# 데이터 테이블
# -------------------------
st.subheader("📊 최근 데이터")
df_display = df.tail(24).copy()
df_display["event_hour"] = df_display["event_hour"].dt.strftime("%Y-%m-%d %H:%M")
df_display["trade_price"] = df_display["trade_price"].round(0)
df_display["trade_volume"] = df_display["trade_volume"].round(2)
df_display = df_display.set_index("event_hour")
st.dataframe(df_display, height=400, use_container_width=True)

# -------------------------
# 모델 성능 지표
# -------------------------
st.subheader("📈 모델 성능 지표")

# 예측 결과의 성능 지표 사용
metrics = prediction_result.get('metrics', {})
metrics_df = pd.DataFrame({
    "모델": [model],
    "RMSE": [f"{metrics.get('rmse', 30000):,.0f}"],
    "MAE": [f"{metrics.get('mae', 20000):,.0f}"],
    "R²": [f"{metrics.get('r2', 0.85):.3f}"],
    "예측 신뢰도": [f"{prediction_confidence:.1%}"],
    "데이터 소스": [prediction_source]
})

st.dataframe(metrics_df, height=100, use_container_width=True)

# -------------------------
# 시스템 상태
# -------------------------
st.subheader("🔧 시스템 상태")
status_cols = st.columns(3)

with status_cols[0]:
    if USE_AWS:
        st.success("🟢 AWS S3 연결됨")
    else:
        st.info("🔵 로컬 더미 데이터")

with status_cols[1]:
    if use_ai_prediction:
        st.success("🟢 AI 예측 모델 활성화")
    else:
        st.warning("🟡 통계 기반 예측 사용")

with status_cols[2]:
    data_quality = "양호" if len(df) >= 20 else "부족"
    if len(df) >= 20:
        st.success(f"🟢 데이터 품질: {data_quality}")
    else:
        st.warning(f"🟡 데이터 품질: {data_quality}")

# 푸터
st.markdown("---")
footer_col1, footer_col2 = st.columns([3, 1])
with footer_col1:
    st.markdown("🤖 **AI 기반 암호화폐 가격 예측 시스템** | 실시간 업데이트")
with footer_col2:
    st.caption(f"마지막 업데이트: {current_hour}")

# 자동 새로고침 옵션 (선택사항)
if st.sidebar.checkbox("자동 새로고침 (30초)", value=False):
    import time
    time.sleep(30)
    st.rerun()
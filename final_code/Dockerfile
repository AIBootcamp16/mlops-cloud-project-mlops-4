# 1. 베이스 이미지
FROM python:3.11-slim

# 2. 작업 디렉토리 생성
WORKDIR /app

# 3. 필수 빌드 도구 설치 (TensorFlow 빌드 종속)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# 4. pip 최신화
RUN pip install --upgrade pip

# 5. 의존성 복사 및 설치 - numpy=2.3.1
RUN pip install --no-cache-dir numpy==1.26.1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 전체 복사
COPY training2.py .
COPY mlflow_model_manager.py .

# 7. 환경변수 (IAM Role 사용 시 AWS 키는 필요 없음)
ENV BUCKET_NAME="raw-data-bucket-moasic-mlops-4"

# 8. 모델 학습 실행
CMD ["python", "training2.py"]

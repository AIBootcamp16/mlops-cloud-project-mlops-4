# 1. 베이스 이미지
FROM python:3.11-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 의존성 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 앱 코드 복사
COPY . .

# 5. Streamlit 포트 노출
EXPOSE 8501

# 6. Streamlit 실행
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
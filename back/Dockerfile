# 1. 베이스 이미지
FROM python:3.11-slim

# 2. 작업 디렉토리 생성
WORKDIR /app

# 3. 의존성 파일 복사
COPY requirements.txt .

# 4. 패키지 설치
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 5. 소스 코드 전체 복사
COPY . .

# 6. 환경변수 설정
ENV PYTHONUNBUFFERED=1

# 7. FastAPI 실행 (python main.py)
CMD ["python", "main.py"]
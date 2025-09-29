# Airflow 공식 이미지 (Python 3.11)
FROM apache/airflow:2.9.3-python3.11

# (선택) 추가 파이썬 패키지 설치
# Airflow는 constraints 파일과 함께 설치해야 충돌이 적습니다.
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
# OS 패키지가 필요하면 여기에 apt-get 추가 (예: curl, vim)
# RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
USER airflow

# requirements.txt가 있으면 제약과 함께 설치
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then \
      pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"; \
    fi

# 기본 작업 디렉터리
WORKDIR /opt/airflow

# 웹서버 8080 노출
EXPOSE 8080

# Airflow standalone로 단일 프로세스 올리기 (웹서버+스케줄러+초기화)
# 비로그인 쉘 없이 바로 실행 가능
CMD ["airflow", "standalone"]

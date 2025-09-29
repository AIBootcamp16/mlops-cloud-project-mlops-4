#!/usr/bin/env bash
set -euo pipefail

# 기본값 (env로 덮어쓰기 가능)
: "${AIRFLOW_ADMIN_USERNAME:=admin}"
: "${AIRFLOW_ADMIN_PASSWORD:=admin}"
: "${AIRFLOW_ADMIN_FIRSTNAME:=Admin}"
: "${AIRFLOW_ADMIN_LASTNAME:=User}"
: "${AIRFLOW_ADMIN_EMAIL:=admin@example.com}"

# 1) DB 초기화 (초기화되어 있으면 알아서 스킵)
airflow db init

# 2) admin 계정 생성(이미 있으면 에러가 나도 계속 진행)
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" || echo "[info] admin user already exists, continue..."

# 3) standalone 실행 (웹서버+스케줄러 한번에)
exec airflow standalone

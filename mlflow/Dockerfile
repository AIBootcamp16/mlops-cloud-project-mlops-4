FROM ghcr.io/mlflow/mlflow:v3.4.0

RUN pip install boto3 psycopg2-binary

WORKDIR /mlflow/
EXPOSE 5000

ARG MLFLOW_BACKEND_STORE_URI
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_BUCKET

ENV MLFLOW_BACKEND_STORE_URI=${MLFLOW_BACKEND_STORE_URI}
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_BUCKET=${AWS_BUCKET}

CMD ["mlflow", "server", "--backend-store-uri", "${MLFLOW_BACKEND_STORE_URI}", "--serve-artifacts", "--artifacts-destination", "s3://${AWS_BUCKET}/mlflow", "--host", "0.0.0.0","--port", "5000"]
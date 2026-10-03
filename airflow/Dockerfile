FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

RUN python -m pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    pandas \
    cassandra-driver \
    psycopg2-binary \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
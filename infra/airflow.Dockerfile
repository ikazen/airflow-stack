FROM apache/airflow:3.2.1

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-edge3 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt"

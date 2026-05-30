FROM apache/airflow:3.2.1

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-edge3==3.6.0" \
    "apache-airflow-providers-git==0.3.1" \
    "apache-airflow-providers-docker==4.5.5"

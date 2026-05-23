FROM apache/airflow:3.2.1

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-edge3==3.6.0" \
    "httpx>=0.28" \
    "beautifulsoup4>=4.12" \
    "lxml>=5.0" \
    "postgrest>=2.0" \
    "python-dotenv>=1.0" \
    "tenacity>=9.0"

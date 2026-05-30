from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from data_sync_common import IMAGE

DOCKER = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    auto_remove="success",
    mount_tmp_dir=False,  # DooD: 워커-host 파일시스템이 달라 tmp mount 깨짐
    environment={
        "DB_URL": "{{ var.value.db_url }}",
        "DB_KEY": "{{ var.value.db_key }}",
    },
    queue="default",
)


@dag(
    dag_id="sync_matches",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 느린 run 이 다음 발화와 겹치지 않도록
    default_args={"execution_timeout": pendulum.duration(minutes=5)},
    tags=["lck-pics", "etl"],
)
def sync_matches() -> None:

    @task.docker(**DOCKER)
    def matches() -> dict:
        from app.tasks import sync_matches

        return sync_matches(force=False)

    matches()


sync_matches()

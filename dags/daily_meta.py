from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from data_sync_common import IMAGE
from lib.alert import notify_discord_on_failure

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
    dag_id="daily_meta",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
        "execution_timeout": pendulum.duration(minutes=10),
    },
    tags=["lck-pics", "etl"],
    on_failure_callback=notify_discord_on_failure,
)
def daily_meta() -> None:

    @task.docker(**DOCKER)
    def leagues() -> dict:
        from app.tasks import sync_leagues

        return sync_leagues()

    @task.docker(**DOCKER)
    def matches() -> dict:
        from app.tasks import sync_matches

        return sync_matches(force=True)

    @task.docker(**DOCKER)
    def secondary() -> dict:
        from app.tasks import sync_secondary

        return sync_secondary(force=True)

    @task.docker(**DOCKER)
    def brackets() -> dict:
        from app.tasks import sync_brackets

        return sync_brackets()

    @task.docker(**DOCKER)
    def report(meta: dict, matches_result: dict, secondary_result: dict, brackets_result: dict) -> dict:
        from app.tasks import report

        return report(meta=meta, matches=matches_result, secondary=secondary_result, brackets=brackets_result)

    meta = leagues()
    mt = matches()
    sc = secondary()
    br = brackets()
    meta >> [mt, sc]  # leagues.is_active 갱신 후 풀 폴링
    mt >> br
    report(meta=meta, matches_result=mt, secondary_result=sc, brackets_result=br)


daily_meta()

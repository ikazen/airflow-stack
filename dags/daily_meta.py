"""일 1회 메타 갱신 + 풀 폴링 (00:00 KST).

leagues/teams 메타를 먼저 갱신한 뒤, 두 소스를 force 로 풀 폴링하여 신규 일정을 발견한다
(평시 active-window 스킵으로 놓친 미래 경기 보강). report 가 세 task 의 xcom 을 수합해
한 줄 요약을 남긴다 (downstream report 패턴).

위상: leagues >> [matches, secondary] >> report
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from data_sync_common import IMAGE

# 이 DAG task 의 실행 환경 (DooD). 실행에 필요한 전부를 DAG 에 명시. 이미지 태그만 공유.
DOCKER = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    auto_remove="success",
    mount_tmp_dir=False,  # DooD 에선 워커-host 파일시스템이 달라 tmp mount 가 깨짐
    # 시크릿은 실행 시점 api-server 경유로 resolve (Task SDK). db_key 는 자동 마스킹.
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
    tags=["lck-pics", "etl"],
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
    def report(meta: dict, matches_result: dict, secondary_result: dict) -> dict:
        from app.tasks import report

        return report(meta=meta, matches=matches_result, secondary=secondary_result)

    meta = leagues()
    mt = matches()
    sc = secondary()
    meta >> [mt, sc]  # leagues.is_active 갱신 후 풀 폴링
    report(meta=meta, matches_result=mt, secondary_result=sc)


daily_meta()

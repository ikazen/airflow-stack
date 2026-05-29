"""보조(외부 위키) 소스 동기화 (15분 간격).

parse API rate limit 때문에 빈 폴링도 비용 — active-window 휴리스틱으로 task 내부 스킵.
신규 페이지/일정은 daily_meta 가 일 1회 force 로 보강.
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
    dag_id="sync_secondary",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["lck-pics", "etl"],
)
def sync_secondary() -> None:

    @task.docker(**DOCKER)
    def secondary() -> dict:
        from app.tasks import sync_secondary

        return sync_secondary(force=False)

    secondary()


sync_secondary()

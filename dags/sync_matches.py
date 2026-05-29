"""경기 일정/결과 동기화 (10분 간격).

active-window 휴리스틱으로 진행/임박 경기 없으면 task 내부에서 즉시 스킵 (외부 API 0회).
신규 일정 발견은 daily_meta 가 일 1회 force 풀 폴링으로 보강.
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
    dag_id="sync_matches",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 느린 run 이 다음 발화와 겹치지 않도록
    tags=["lck-pics", "etl"],
)
def sync_matches() -> None:

    @task.docker(**DOCKER)
    def matches() -> dict:
        from app.tasks import sync_matches

        return sync_matches(force=False)

    matches()


sync_matches()

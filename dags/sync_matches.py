"""경기 일정/결과 동기화 (10분 간격).

active-window 휴리스틱으로 진행/임박 경기 없으면 task 내부에서 즉시 스킵 (외부 API 0회).
신규 일정 발견은 daily_meta 가 일 1회 force 풀 폴링으로 보강.
"""
from __future__ import annotations

from airflow.sdk import dag, task
from data_sync_common import DOCKER_KWARGS, START_DATE, TAGS


@dag(
    dag_id="sync_matches",
    schedule="*/10 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,  # 느린 run 이 다음 발화와 겹치지 않도록
    tags=TAGS,
)
def sync_matches() -> None:

    @task.docker(**DOCKER_KWARGS)
    def matches() -> dict:
        from app.tasks import sync_matches

        return sync_matches(force=False)

    matches()


sync_matches()

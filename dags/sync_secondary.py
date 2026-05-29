"""보조(외부 위키) 소스 동기화 (15분 간격).

parse API rate limit 때문에 빈 폴링도 비용 — active-window 휴리스틱으로 task 내부 스킵.
신규 페이지/일정은 daily_meta 가 일 1회 force 로 보강.
"""
from __future__ import annotations

from airflow.sdk import dag, task
from data_sync_common import DOCKER_KWARGS, START_DATE, TAGS


@dag(
    dag_id="sync_secondary",
    schedule="*/15 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
)
def sync_secondary() -> None:

    @task.docker(**DOCKER_KWARGS)
    def secondary() -> dict:
        from app.tasks import sync_secondary

        return sync_secondary(force=False)

    secondary()


sync_secondary()

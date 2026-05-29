"""일 1회 메타 갱신 + 풀 폴링 (00:00 KST).

leagues/teams 메타를 먼저 갱신한 뒤, 두 소스를 force 로 풀 폴링하여 신규 일정을 발견한다
(평시 active-window 스킵으로 놓친 미래 경기 보강). report 가 세 task 의 xcom 을 수합해
한 줄 요약을 남긴다 (downstream report 패턴).

위상: leagues >> [matches, secondary] >> report
"""
from __future__ import annotations

from airflow.sdk import dag, task
from data_sync_common import DOCKER_KWARGS, START_DATE, TAGS


@dag(
    dag_id="daily_meta",
    schedule="0 0 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
)
def daily_meta() -> None:

    @task.docker(**DOCKER_KWARGS)
    def leagues() -> dict:
        from app.tasks import sync_leagues

        return sync_leagues()

    @task.docker(**DOCKER_KWARGS)
    def matches() -> dict:
        from app.tasks import sync_matches

        return sync_matches(force=True)

    @task.docker(**DOCKER_KWARGS)
    def secondary() -> dict:
        from app.tasks import sync_secondary

        return sync_secondary(force=True)

    @task.docker(**DOCKER_KWARGS)
    def report(meta: dict, matches_result: dict, secondary_result: dict) -> dict:
        from app.tasks import report

        return report(meta=meta, matches=matches_result, secondary=secondary_result)

    meta = leagues()
    mt = matches()
    sc = secondary()
    meta >> [mt, sc]  # leagues.is_active 갱신 후 풀 폴링
    report(meta=meta, matches_result=mt, secondary_result=sc)


daily_meta()

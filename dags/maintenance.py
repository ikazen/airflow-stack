from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from lib.alert import notify_discord_on_failure

LOG_DIR = Path("/opt/airflow/logs")
RETENTION_DAYS = 14


@dag(
    dag_id="maintenance",
    schedule="0 6 * * 3",  # cron 은 start_date tz 기준 — KST 의도면 tz-aware 필수
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["ops"],
    on_failure_callback=notify_discord_on_failure,
)
def maintenance() -> None:
    # queue="ops": 로그 볼륨이 ops-vm 에 있어 ops 큐에서 실행해야 함
    # db clean 은 task 불가 (Task SDK 는 DB 접속 안 줌) → runbook.md

    @task(queue="ops")
    def cleanup_task_logs(retention_days: int = RETENTION_DAYS) -> int:
        cutoff = pendulum.now("UTC").subtract(days=retention_days).timestamp()
        deleted = 0
        for f in LOG_DIR.rglob("*.log"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        for d in sorted(LOG_DIR.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        return deleted

    cleanup_task_logs()


maintenance()

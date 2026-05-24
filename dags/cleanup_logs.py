from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops"],
)
def cleanup_logs() -> None:
    @task(queue="ops")
    def delete_old_task_logs(days_to_keep: int = 30) -> int:
        log_dir = Path("/opt/airflow/logs")
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        deleted = 0
        for f in log_dir.rglob("*.log"):
            if f.stat().st_mtime < cutoff.timestamp():
                f.unlink()
                deleted += 1
        for d in sorted(log_dir.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        return deleted

    delete_old_task_logs()


cleanup_logs()

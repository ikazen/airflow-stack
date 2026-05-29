from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task

LOG_DIR = Path("/opt/airflow/logs")
RETENTION_DAYS = 14


@dag(
    dag_id="maintenance",
    # 매주 수요일 06:00 KST. cron 은 start_date 의 tz 로 해석됨 (tz-aware 필수).
    schedule="0 6 * * 3",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["ops"],
)
def maintenance() -> None:
    # queue="ops" — task 로그가 모이는 ops-vm 중앙 로그 볼륨에서 실행돼야 함.
    # 메타 DB 정리(db clean)는 task 로 불가 (Task SDK 가 task 에 DB 접속을 안 줌) → host-level, docs/runbook.md 참조.

    @task(queue="ops")
    def cleanup_task_logs(retention_days: int = RETENTION_DAYS) -> int:
        """RETENTION_DAYS 보다 오래된 task 로그 파일 삭제 + 빈 디렉토리 정리."""
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

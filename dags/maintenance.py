from __future__ import annotations

import subprocess
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

LOG_DIR = Path("/opt/airflow/logs")
RETENTION_DAYS = 30


@dag(
    dag_id="maintenance",
    # 매주 수요일 06:00 KST. cron 은 start_date 의 tz 로 해석됨 (tz-aware 필수).
    schedule="0 6 * * 3",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["ops"],
)
def maintenance() -> None:
    # 둘 다 queue="ops" — ops-vm 은 중앙 로그 볼륨 + 메타 DB 접근을 가진 컨트롤 플레인.
    # NAT 뒤 도메인 워커(worker-vm/mac-server)는 DB 안 침 (CLAUDE.md Edge Executor 원칙).

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

    @task(queue="ops")
    def db_clean(retention_days: int = RETENTION_DAYS) -> None:
        """메타 DB 의 오래된 행 purge. disposable DB 라 --skip-archive."""
        cutoff = pendulum.now("UTC").subtract(days=retention_days).isoformat()
        subprocess.run(
            ["airflow", "db", "clean", "--clean-before-timestamp", cutoff, "--skip-archive", "-y"],
            check=True,
        )

    cleanup_task_logs()
    db_clean()


maintenance()

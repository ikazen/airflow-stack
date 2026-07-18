from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from lib.alert import notify_discord_on_failure

LOG_DIR = Path("/opt/airflow/logs")
RETENTION_DAYS = 14


@dag(
    dag_id="maint_airflow",
    schedule="0 6 * * 3",  # cron 은 start_date tz 기준 — KST 의도면 tz-aware 필수
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["ops"],
    on_failure_callback=notify_discord_on_failure,
)
def maint_airflow() -> None:
    # queue="ops-vm": 로그 볼륨이 ops-vm 에 있어 ops-vm 큐에서 실행해야 함

    @task(queue="ops-vm")
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

    # db clean 은 task 프로세스 자체에서 불가 — Task SDK 가 SQL_ALCHEMY_CONN 을 안 줌.
    # 대신 DooD exec 로 DB 권한을 가진 scheduler 컨테이너에서 실행 (maint_registry
    # garbage_collect 와 동일 패턴). 수동 fallback 절차는 runbook.md 참조.
    @task(queue="ops-vm", execution_timeout=pendulum.duration(minutes=10))
    def db_clean(retention_days: int = RETENTION_DAYS) -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        cutoff = pendulum.now("UTC").subtract(days=retention_days).to_iso8601_string()
        result = client.containers.get("airflow-scheduler-1").exec_run(
            ["airflow", "db", "clean", "--clean-before-timestamp", cutoff, "--skip-archive", "-y"]
        )
        output = result.output.decode() if result.output else ""
        print(output)
        if result.exit_code != 0:
            raise RuntimeError(f"db clean failed (exit {result.exit_code})")

    cleanup_task_logs()
    db_clean()


maint_airflow()

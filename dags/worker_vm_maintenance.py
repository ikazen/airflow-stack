from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

QUEUE = "maint-worker-vm"
PRUNE_UNTIL = "168h"


@dag(
    dag_id="worker_vm_maintenance",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["ops"],
)
def worker_vm_maintenance() -> None:
    @task(queue=QUEUE, execution_timeout=pendulum.duration(minutes=15))
    def prune_images() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.images.prune(filters={"dangling": False, "until": PRUNE_UNTIL})
        print(f"images pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    @task(queue=QUEUE, execution_timeout=pendulum.duration(minutes=10))
    def prune_build_cache() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.api.prune_builds(filters={"until": PRUNE_UNTIL})
        print(f"build cache pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    prune_images()
    prune_build_cache()


worker_vm_maintenance()

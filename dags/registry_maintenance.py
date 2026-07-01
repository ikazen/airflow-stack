from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from lib.alert import notify_discord_on_failure

REGISTRY_URL = "http://registry:5000"
REGISTRY_KEEP = 5


@dag(
    dag_id="registry_maintenance",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["ops"],
    on_failure_callback=notify_discord_on_failure,
)
def registry_maintenance() -> None:
    # 모든 task 는 queue="ops" — docker.sock 이 ops-vm 워커에만 마운트됨.
    # ops 큐 = privileged 인프라 유지보수 전용, 일반 워크로드 라우팅 금지 (decisions L28).

    @task(queue="ops", execution_timeout=pendulum.duration(minutes=10))
    def prune_manifests() -> int:
        import registry_retention

        return registry_retention.run(REGISTRY_URL, keep=REGISTRY_KEEP)

    @task(queue="ops", execution_timeout=pendulum.duration(minutes=30))
    def garbage_collect() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.containers.get("registry").exec_run(
            "registry garbage-collect -m /etc/docker/registry/config.yml"
        )
        output = result.output.decode() if result.output else ""
        print(output)
        if result.exit_code != 0:
            raise RuntimeError(f"garbage-collect failed (exit {result.exit_code})")

    @task(queue="ops", execution_timeout=pendulum.duration(minutes=10))
    def builder_prune() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.api.prune_builds(filters={"until": "168h"})
        reclaimed = result.get("SpaceReclaimed", 0)
        print(f"build cache pruned: {reclaimed / 1024 / 1024:.1f} MB reclaimed")

    prune_manifests() >> garbage_collect()
    builder_prune()


registry_maintenance()

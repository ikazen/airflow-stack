from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from lib.alert import notify_discord_on_failure

REGISTRY_URL = "http://registry:5000"
REGISTRY_KEEP = 5
PRUNE_UNTIL = "168h"


@dag(
    dag_id="maint_registry",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["ops"],
    on_failure_callback=notify_discord_on_failure,
)
def maint_registry() -> None:
    # 노드별 docker.sock DooD 유지보수 — registry(ops-vm) GC/retention + 각 노드
    # 로컬 이미지·build cache prune. 세 노드 task 는 서로 독립, 각자 큐에서 병렬 실행.
    # ops 큐 = privileged 인프라 유지보수 전용, 일반 워크로드 라우팅 금지 (decisions L28).

    @task(queue="ops-vm", execution_timeout=pendulum.duration(minutes=10))
    def prune_manifests() -> int:
        import registry_retention

        return registry_retention.run(REGISTRY_URL, keep=REGISTRY_KEEP)

    @task(queue="ops-vm", execution_timeout=pendulum.duration(minutes=30))
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

    @task(queue="ops-vm", execution_timeout=pendulum.duration(minutes=10))
    def registry_builder_prune() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.api.prune_builds(filters={"until": PRUNE_UNTIL})
        reclaimed = result.get("SpaceReclaimed", 0)
        print(f"build cache pruned: {reclaimed / 1024 / 1024:.1f} MB reclaimed")

    @task(queue="worker-vm", execution_timeout=pendulum.duration(minutes=15))
    def worker_vm_prune_images() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.images.prune(filters={"dangling": False, "until": PRUNE_UNTIL})
        print(f"images pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    @task(queue="worker-vm", execution_timeout=pendulum.duration(minutes=10))
    def worker_vm_prune_build_cache() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.api.prune_builds(filters={"until": PRUNE_UNTIL})
        print(f"build cache pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    @task(queue="mac-server", execution_timeout=pendulum.duration(minutes=15))
    def mac_server_prune_images() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.images.prune(filters={"dangling": False, "until": PRUNE_UNTIL})
        print(f"images pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    @task(queue="mac-server", execution_timeout=pendulum.duration(minutes=10))
    def mac_server_prune_build_cache() -> None:
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        result = client.api.prune_builds(filters={"until": PRUNE_UNTIL})
        print(f"build cache pruned: {result.get('SpaceReclaimed', 0) / 1024 / 1024:.1f} MB")

    prune_manifests() >> garbage_collect()
    registry_builder_prune()
    worker_vm_prune_images()
    worker_vm_prune_build_cache()
    mac_server_prune_images()
    mac_server_prune_build_cache()


maint_registry()

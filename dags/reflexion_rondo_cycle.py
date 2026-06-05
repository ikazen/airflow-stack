"""reflexion-rondo single-cycle DAG.

DAG conf: {competition_id, stage, queue_id}
1 DAG run = 1 cycle (Strategist → Coder → Evaluator → Reflector).
daemon이 큐에서 아이템을 꺼내 trigger하고, DAG 완료 후 DB에서 결과를 읽는다.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

IMAGE = "registry.internal:80/reflexion-rondo/daemon:latest"

_DOCKER_BASE = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    auto_remove="success",
    mount_tmp_dir=False,
    mounts=[
        Mount(source="/var/lib/rondo", target="/app/runs", type="bind"),
        Mount(source="/var/lib/rondo/data", target="/app/data", type="bind"),
        Mount(source="/var/lib/rondo/.env", target="/app/.env", type="bind"),
        Mount(source="/tmp/rondo-eval", target="/tmp/rondo-eval", type="bind"),
        Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
    ],
    queue="default",
)


@dag(
    dag_id="reflexion_rondo_cycle",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=4,
    tags=["rondo"],
)
def reflexion_rondo_cycle() -> None:

    DockerOperator(
        task_id="run_attempt",
        command=(
            "uv run python -m bin.run_cycle_task"
            " --competition {{ dag_run.conf['competition_id'] }}"
            " --stage {{ dag_run.conf['stage'] }}"
            " --queue-id {{ dag_run.conf['queue_id'] }}"
        ),
        **_DOCKER_BASE,
    )


reflexion_rondo_cycle()

"""reflexion-rondo super-cycle DAG — BON-110.

DAG conf: {competition_id, stage, queue_id}
1 DAG run = 1 super-cycle:
  retrieve → [attempt_0, attempt_1, attempt_2] → promote

daemon이 큐에서 아이템을 꺼내 trigger하고, DAG 완료 후 DB에서 결과를 읽는다.
시크릿은 Airflow Variable로 주입 (var.value.xxx) — .env 마운트 없음.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator as _DockerBase


class DockerOperator(_DockerBase):
    template_fields = ("command", "environment")


IMAGE = "registry.internal:80/reflexion-rondo/task:v1.1.1"

_DOCKER_BASE = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    auto_remove="success",
    mount_tmp_dir=False,
)

_DOCKER_LIGHT = dict(**_DOCKER_BASE, queue="default", cpus=0.5)
_DOCKER_HEAVY = dict(**_DOCKER_BASE, queue="big", cpus=1.5)

_ENV = {
    "PYTHONUNBUFFERED":        "1",
    "RONDO_DB_URL":            "{{ var.value.rondo_db_url }}",
    "OLLAMA_BASE_URL":         "{{ var.value.ollama_base_url }}",
    "OLLAMA_CLOUD_BASE_URL":   "{{ var.value.ollama_cloud_base_url }}",
    "OLLAMA_API_KEY":          "{{ var.value.ollama_api_key }}",
    "MINIO_ENDPOINT":          "{{ var.value.minio_endpoint }}",
}


@dag(
    dag_id="reflexion_rondo_cycle",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=4,
    tags=["rondo"],
)
def reflexion_rondo_cycle() -> None:
    retrieve = DockerOperator(
        task_id="retrieve",
        command=(
            "uv run --no-sync python -m bin.run_retrieve_task"
            " --competition {{ dag_run.conf['competition_id'] }}"
            " --stage {{ dag_run.conf['stage'] }}"
            " --queue-id {{ dag_run.conf['queue_id'] }}"
        ),
        environment=_ENV,
        **_DOCKER_LIGHT,
    )

    attempts = [
        DockerOperator(
            task_id=f"attempt_{i}",
            command=(
                "uv run --no-sync python -m bin.run_attempt_task"
                " --competition {{ dag_run.conf['competition_id'] }}"
                " --stage {{ dag_run.conf['stage'] }}"
                " --queue-id {{ dag_run.conf['queue_id'] }}"
                f" --attempt-index {i}"
            ),
            environment=_ENV,
            **_DOCKER_HEAVY,
        )
        for i in range(3)
    ]

    promote = DockerOperator(
        task_id="promote",
        command=(
            "uv run --no-sync python -m bin.run_promote_task"
            " --queue-id {{ dag_run.conf['queue_id'] }}"
        ),
        environment=_ENV,
        **_DOCKER_LIGHT,
    )

    retrieve >> attempts >> promote


reflexion_rondo_cycle()

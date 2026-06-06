"""reflexion-rondo single-cycle DAG.

DAG conf: {competition_id, stage, queue_id}
1 DAG run = 1 cycle (Strategist → Coder → Evaluator → Reflector).
daemon이 큐에서 아이템을 꺼내 trigger하고, DAG 완료 후 DB에서 결과를 읽는다.

시크릿은 Airflow Variable로 주입 (var.value.xxx) — .env 마운트 없음.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator as _DockerBase


class DockerOperator(_DockerBase):
    template_fields = ("command", "environment")


IMAGE = "registry.internal:80/reflexion-rondo/daemon:latest"

_DOCKER_BASE = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    auto_remove="success",
    mount_tmp_dir=False,
    cpus=1.5,
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
        environment={
            "RONDO_DB_URL":            "{{ var.value.rondo_db_url }}",
            "OLLAMA_BASE_URL":         "{{ var.value.ollama_base_url }}",
            "OLLAMA_CLOUD_BASE_URL":   "{{ var.value.ollama_cloud_base_url }}",
            "OLLAMA_API_KEY":          "{{ var.value.ollama_api_key }}",
            "MODEL_STRATEGIST":        "{{ var.value.rondo_model_strategist }}",
            "MODEL_REFLECTOR":         "{{ var.value.rondo_model_reflector }}",
            "MODEL_CODER":             "{{ var.value.rondo_model_coder }}",
            "MODEL_EMBEDDING":         "{{ var.value.rondo_model_embedding }}",
            "MINIO_ENDPOINT":          "{{ var.value.minio_endpoint }}",
        },
        **_DOCKER_BASE,
    )


reflexion_rondo_cycle()

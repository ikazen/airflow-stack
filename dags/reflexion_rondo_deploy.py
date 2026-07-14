"""reflexion-rondo 이미지 빌드+push DAG — 수동 트리거, daemon+task 두 이미지.

dag_run.conf: {"tag": "v1.2.27"} — Airflow UI "Trigger DAG w/ config" 폼이 곧 배포 UI.

이 DAG는 이미지를 registry.internal:5000에 빌드+push하고 task 이미지를 즉시
반영(rondo_task_image_version Variable bump)하는 것까지만 한다. daemon의 실제
배포(compose.yml 태그 bump+재시작)는 이 DAG가 하지 않는다 — reflexion-rondo
`deploy/release.sh`(WSL, 사용자 로컬 git credential)의 몫으로 남긴다. build만
해두면 release.sh는 그 태그를 지정해 compose.yml만 bump하면 되므로 ops-vm SSH
build 단계가 필요 없어진다.

새 credential 불필요 — reflexion-rondo는 public repo라 clone에 인증이 없고,
registry.internal:5000은 무인증(HTTP insecure, tailnet 경계로만 보호), task
Variable bump는 Airflow 자체 API. ops 큐 docker.sock 재사용 범위 확장은
docs/decisions.md L28 amendment 참조.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Variable, dag, get_current_context, task
from lib.alert import notify_discord_on_failure
from lib.image_deploy import build_and_push

_REPO_URL = "https://github.com/ikazen/reflexion-rondo.git"
_AIRFLOW_URL = "http://airflow-api-server-1:8080"

# reflexion_rondo_cycle.py의 _ENV와 동일한 Variable 집합 재사용 — 새 Variable 불필요.
_PREFLIGHT_ENV_VARS = {
    "RONDO_DB_URL": "rondo_db_url",
    "OLLAMA_BASE_URL": "ollama_base_url",
    "OLLAMA_CLOUD_BASE_URL": "ollama_cloud_base_url",
    "OLLAMA_API_KEY": "ollama_api_key",
    "MINIO_ENDPOINT": "minio_endpoint",
}


@dag(
    dag_id="reflexion_rondo_deploy",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["rondo", "ops"],
    on_failure_callback=notify_discord_on_failure,
)
def reflexion_rondo_deploy() -> None:
    @task(queue="ops", execution_timeout=timedelta(minutes=20))
    def build_daemon() -> str:
        tag = get_current_context()["dag_run"].conf["tag"]
        return build_and_push(
            repo_url=_REPO_URL, ref="main",
            dockerfile="deploy/Dockerfile", image_repo="reflexion-rondo/daemon", tag=tag,
        )

    @task(queue="ops", execution_timeout=timedelta(minutes=20))
    def build_task() -> str:
        tag = get_current_context()["dag_run"].conf["tag"]
        return build_and_push(
            repo_url=_REPO_URL, ref="main",
            dockerfile="deploy/Dockerfile.task", image_repo="reflexion-rondo/task", tag=tag,
        )

    @task(queue="ops", execution_timeout=timedelta(minutes=5), trigger_rule="all_success")
    def preflight(daemon_image: str, task_image: str) -> None:
        """일회성 컨테이너로 두 이미지 검증 — deploy/release.sh 사전검증(issue #15)과 동일 로직."""
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        env = {env_key: Variable.get(var_key) for env_key, var_key in _PREFLIGHT_ENV_VARS.items()}
        env["AIRFLOW_URL"] = _AIRFLOW_URL

        client.containers.run(
            daemon_image,
            "uv run --no-sync python -m bin.healthcheck --skip ollama_local",
            network="nexus",
            environment=env,
            remove=True,
        )
        client.containers.run(
            task_image,
            "uv run --no-sync python -c \""
            "from evaluator.harness import BasePipeline, PatchedPipeline, PipelineContext, evaluate_pipeline; "
            "from runtime import runner; "
            "import polars, sklearn, lightgbm, catboost, xgboost; "
            "print('task image import OK')\"",
            remove=True,
        )

    @task
    def bump_task_variable() -> None:
        tag = get_current_context()["dag_run"].conf["tag"]
        Variable.set("rondo_task_image_version", tag)

    daemon_image = build_daemon()
    task_image = build_task()
    preflight(daemon_image, task_image) >> bump_task_variable()


reflexion_rondo_deploy()

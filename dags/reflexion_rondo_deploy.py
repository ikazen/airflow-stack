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

# issue #12: reflexion_rondo_cycle.py의 _ENV(Airflow Variable 기반, rondo_db_url 등)는
# cross-host 접근용 값이다(worker-vm/mac-server가 Tailscale로 ops-vm Postgres에 닿기 위한
# tailnet IP). preflight 컨테이너는 network="nexus"로 ops-vm 자기 자신에서 뜨므로 그 값을
# 쓰면 "No route to host"가 난다 — 게다가 AIRFLOW_USER/PASSWORD도 아예 없어서 airflow
# 헬스체크가 400을 낸다. 대신 ops-vm 호스트의 /var/lib/rondo/.env(reflexion-rondo daemon이
# 실제로 쓰는 파일, nexus 내부용 값 + AIRFLOW_USER/PASSWORD 포함 — deploy/release.sh의
# 기존 사전검증이 --env-file로 이미 성공적으로 쓰고 있음)를 그대로 읽어 같은 진실
# 소스로 통일한다. edge-worker-ops에 이 경로가 볼륨 마운트돼 있어야 한다
# (infra/ops-vm/docker-compose.yml).
_RONDO_ENV_PATH = "/var/lib/rondo/.env"


def _load_rondo_env() -> dict[str, str]:
    env: dict[str, str] = {}
    with open(_RONDO_ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k] = v
    return env


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
        """일회성 컨테이너로 두 이미지 검증 — deploy/release.sh 사전검증(issue #15)과 동일 로직·동일 env 소스."""
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        env = _load_rondo_env()

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

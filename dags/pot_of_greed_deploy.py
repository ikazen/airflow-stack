"""pot-of-greed 이미지 빌드+push DAG — 수동 트리거, api+ui 두 이미지.

dag_run.conf: {"tag": "v0.1.0"} — Airflow UI "Trigger DAG w/ config" 폼이 곧 배포 UI.

이 DAG는 두 이미지를 registry.internal:5000에 빌드+push하는 것까지만 한다. 실제
컷오버(nexus-prime compose 태그 bump + ops-vm 재시작)는 이 DAG가 하지 않는다 —
nexus-prime `scripts/release-pog.sh`(WSL, SSH 기반)의 몫으로 남긴다. reflexion_rondo_deploy와
동일한 build/cutover 분리 구조.

빌드는 로컬 WSL이 아니라 ops-vm 큐 edge-worker(docker.sock 마운트)에서 돈다 —
로컬 dockerd의 insecure-registry 설정 부담 없이 registry에 직접 push할 수 있다.

새 credential 불필요 — pot-of-greed는 public repo라 clone에 인증이 없고,
registry.internal:5000은 무인증(HTTP insecure, tailnet 경계로만 보호). 공용 빌드
헬퍼는 lib/image_deploy.py(reflexion_rondo_deploy와 공유). ui 이미지는 빌드 컨텍스트가
`ui/`라 context_subdir로 지정한다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from lib.alert import notify_discord_on_failure
from lib.image_deploy import build_and_push

_REPO_URL = "https://github.com/ikazen/pot-of-greed.git"


@dag(
    dag_id="pot_of_greed_deploy",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pot-of-greed", "ops"],
    on_failure_callback=notify_discord_on_failure,
)
def pot_of_greed_deploy() -> None:
    @task(queue="ops-vm", execution_timeout=timedelta(minutes=20))
    def build_api() -> str:
        tag = get_current_context()["dag_run"].conf["tag"]
        return build_and_push(
            repo_url=_REPO_URL, ref="main",
            dockerfile="Dockerfile", image_repo="pot-of-greed", tag=tag,
        )

    @task(queue="ops-vm", execution_timeout=timedelta(minutes=20))
    def build_ui() -> str:
        tag = get_current_context()["dag_run"].conf["tag"]
        return build_and_push(
            repo_url=_REPO_URL, ref="main",
            dockerfile="Dockerfile", context_subdir="ui",
            image_repo="pot-of-greed-ui", tag=tag,
        )

    build_api()
    build_ui()


pot_of_greed_deploy()

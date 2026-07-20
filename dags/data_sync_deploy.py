"""lck-pics data-sync 이미지 빌드+push DAG — 수동 트리거, 단일 이미지.

dag_run.conf: {"tag": "v0.1.0"} — Airflow UI "Trigger DAG w/ config" 폼이 곧 배포 UI.

이 DAG는 이미지를 registry.internal:5000에 빌드+push하고 즉시
data_sync_image_version Variable을 bump하는 것까지 한다(reflexion_rondo_deploy와
달리 별도 cutover 단계가 없다 — sync_matches/sync_secondary/daily_meta 세 DAG가
공유하는 이 Variable을 bump하면 다음 task 실행부터 새 이미지를 쓴다).

lck-pics는 private repo라 clone에 인증이 필요하다(reflexion-rondo/pot-of-greed는
public repo라 이 경로를 처음 실사용). image_deploy.build_and_push의
private_pat_var로 read-only PAT(Airflow Variable lck_pics_repo_pat)를 주입한다.
registry.internal:5000은 기존과 동일 무인증(HTTP insecure, tailnet 경계로만 보호).

기존에는 M1 mac에서 scripts/build-and-push.sh로 수동 빌드+push 후 Variable도
사람이 직접 bump했다 — 이 갭(머지됐지만 이미지 재배포 누락)이 KESPA Cup 2026
LIVE 표시 버그의 근본 원인이었다(lck-pics#4 후속). build-and-push.sh는
registry/인프라 장애 시 긴급 로컬 빌드용 fallback으로 남겨둔다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Variable, dag, get_current_context, task
from lib.alert import notify_discord_on_failure
from lib.image_deploy import build_and_push

_REPO_URL = "https://github.com/ikazen/lck-pics.git"
_PAT_VAR = "lck_pics_repo_pat"


@dag(
    dag_id="data_sync_deploy",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["lck-pics", "ops"],
    on_failure_callback=notify_discord_on_failure,
)
def data_sync_deploy() -> None:
    @task(queue="ops-vm", execution_timeout=timedelta(minutes=20))
    def build() -> str:
        tag = get_current_context()["dag_run"].conf["tag"]
        return build_and_push(
            repo_url=_REPO_URL, ref="main",
            dockerfile="docker/Dockerfile", image_repo="lck-pics/data-sync",
            tag=tag, private_pat_var=_PAT_VAR,
        )

    @task(queue="ops-vm", execution_timeout=timedelta(minutes=5))
    def preflight(image: str) -> None:
        """일회성 컨테이너로 이미지 검증 — Dockerfile CMD(app.tasks import)가 곧 헬스체크."""
        import docker

        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        client.containers.run(image, remove=True)

    @task
    def bump_variable() -> None:
        tag = get_current_context()["dag_run"].conf["tag"]
        Variable.set("data_sync_image_version", tag)

    image = build()
    preflight(image) >> bump_variable()


data_sync_deploy()

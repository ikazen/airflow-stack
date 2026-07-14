"""공용 이미지 빌드+push 헬퍼 — 여러 repo의 배포 DAG가 재사용.

ops 큐(ops-vm edge-worker, docker.sock 마운트)에서 실행되는 @task 콜러블 안에서만
호출할 것. registry_maintenance.py/worker_vm_maintenance.py 와 동일하게 docker
python SDK(`docker.DockerClient(base_url="unix://var/run/docker.sock")`)를 쓴다 —
CLI 바이너리 대신 SDK를 쓰는 게 이 repo의 기존 DooD 컨벤션(decisions.md L28).

`docker`(pip 패키지)는 함수 본문 안에서만 import 한다 — dag-processor 가 이 모듈을
파싱 시점에 import 하므로(`from lib.image_deploy import build_and_push`), 도메인
의존성을 모듈 최상단에 두면 dag-processor 환경에 불필요한 요구가 생긴다
(CLAUDE.md "@task 안 import" 컨벤션과 동일한 이유).

registry.internal:5000 은 무인증(HTTP insecure, tailnet 경계로만 보호) — push 에
credential 이 필요 없다. public repo clone 도 credential 불필요. private repo 는
`private_pat_var`로 지정한 Airflow Variable(read-only PAT)을 clone URL에 삽입한다
(현재 이 헬퍼를 쓰는 DAG 중 private repo 대상은 없음 — 필요해질 때 값만 채우면 됨).
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile

REGISTRY = "registry.internal:5000"


def build_and_push(
    *,
    repo_url: str,
    ref: str,
    dockerfile: str,
    image_repo: str,
    tag: str,
    private_pat_var: str | None = None,
) -> str:
    """repo_url의 ref를 clone → dockerfile로 빌드 → {REGISTRY}/{image_repo}:{tag} push.

    반환값: push된 전체 이미지 참조 문자열.
    """
    import docker

    clone_url = repo_url
    if private_pat_var:
        from airflow.sdk import Variable

        pat = Variable.get(private_pat_var)
        # https://<token>@github.com/... 형태로 삽입 (github PAT clone 관용 표기)
        scheme, _, rest = repo_url.partition("://")
        clone_url = f"{scheme}://{pat}@{rest}"

    workdir = tempfile.mkdtemp(prefix="image-deploy-")
    try:
        result = subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, clone_url, workdir],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git clone failed rc={result.returncode} stderr={result.stderr[:1000]}")

        image_ref = f"{REGISTRY}/{image_repo}:{tag}"
        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        _, build_logs = client.images.build(
            path=workdir,
            dockerfile=dockerfile,
            tag=image_ref,
            rm=True,
        )
        for chunk in build_logs:
            if "stream" in chunk:
                print(chunk["stream"], end="")

        for line in client.images.push(f"{REGISTRY}/{image_repo}", tag=tag, stream=True, decode=True):
            if "error" in line:
                raise RuntimeError(f"push failed: {line['error']}")
            if "status" in line:
                print(f"{line['status']} {line.get('progress', '')}")

        return image_ref
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

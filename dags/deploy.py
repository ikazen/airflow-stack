from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow.models.param import Param
from airflow.sdk import Variable, dag, get_current_context, task


def _git_deploy(repo: str, commit: str) -> None:
    url = Variable.get(f"repo_{repo}_url")
    local_path = Variable.get(f"repo_{repo}_local_path")

    if not os.path.isdir(os.path.join(local_path, ".git")):
        print(f"cloning {repo} into {local_path}")
        subprocess.run(["git", "clone", url, local_path], check=True)

    subprocess.run(["git", "-C", local_path, "fetch", "origin"], check=True)

    if commit:
        print(f"checkout {commit}")
        subprocess.run(["git", "-C", local_path, "checkout", commit], check=True)
    else:
        print("pull origin/main")
        subprocess.run(["git", "-C", local_path, "checkout", "main"], check=True)
        subprocess.run(["git", "-C", local_path, "pull", "origin", "main"], check=True)

    head = subprocess.run(
        ["git", "-C", local_path, "log", "--oneline", "-1"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    print(f"[{repo}] HEAD: {head}")


@dag(
    dag_id="deploy",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "repo": Param(
            "airflow-stack",
            type="string",
            description="배포할 repo 이름 (예: airflow-stack)",
        ),
        "commit": Param(
            "",
            type="string",
            description="commit hash 또는 tag. 비우면 origin/main HEAD.",
        ),
    },
    tags=["ops"],
)
def deploy() -> None:

    @task(queue="ops")
    def deploy_ops() -> None:
        ctx = get_current_context()
        _git_deploy(ctx["params"]["repo"], ctx["params"]["commit"])

    @task(queue="default")
    def deploy_worker() -> None:
        ctx = get_current_context()
        _git_deploy(ctx["params"]["repo"], ctx["params"]["commit"])

    # 양쪽 VM 동시 배포
    deploy_ops()
    deploy_worker()


deploy()

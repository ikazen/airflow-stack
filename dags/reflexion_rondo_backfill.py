"""reflexion-rondo materialized_code 행동 검증 백필 DAG — reflexion-rondo#254.

스냅샷 없는 승격 행의 병합본을 오늘 로직으로 재생·재평가해 raw.pipelines에 신뢰
스냅샷(materialized_code/materialized_sha256/materialized_origin)을 채운다. 재현 불가한
행은 verdict만 기록하거나(데이터 이동) 격리한다(cv mismatch). 상세: bin/backfill_materialized_code.py.

dag_run.conf:
  {"competition": "playground-series-s4e12"}                    dry-run (기본, DB 안 건드림)
  {"competition": "playground-series-s4e10", "apply": true,
   "allow_chain": true, "remeasure": true}                      실제 반영 + 약한 tier + cv 재작성
  competition 생략 시 active_competition_ids() 전체.

수동 트리거 전용. eval-heavy(대회당 승격 행 수 x p50 350-800s CPU) — reflexion_rondo_cycle.py의
attempt 컨테이너와 동일 이미지·queue="big", cap_add=SYS_ADMIN(runtime/isolate.py의
os.unshare(CLONE_NEWNET) 요구). ops-vm(daemon 상주, 2 OCPU)이 아니라 big 큐에서 돈다.
새 bin 스크립트는 reflexion_rondo_deploy로 이미지 태그가 bump된 뒤에만 존재한다 —
deploy DAG를 먼저 돌릴 것.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator as _DockerBase
from lib.alert import notify_discord_on_failure


class DockerOperator(_DockerBase):
    template_fields = ("command", "environment", "image")


IMAGE = "registry.internal:5000/reflexion-rondo/task:{{ var.value.rondo_task_image_version }}"

_ENV = {
    "PYTHONUNBUFFERED":      "1",
    "RONDO_DB_URL":          "{{ var.value.rondo_db_url }}",
    "OLLAMA_BASE_URL":       "{{ var.value.ollama_base_url }}",
    "OLLAMA_CLOUD_BASE_URL": "{{ var.value.ollama_cloud_base_url }}",
    "OLLAMA_API_KEY":        "{{ var.value.ollama_api_key }}",
    "MINIO_ENDPOINT":        "{{ var.value.minio_endpoint }}",
}


@dag(
    dag_id="reflexion_rondo_backfill_materialized",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["rondo"],
    on_failure_callback=notify_discord_on_failure,
)
def reflexion_rondo_backfill_materialized() -> None:
    DockerOperator(
        task_id="backfill",
        command=(
            "uv run --no-sync python -m bin.backfill_materialized_code"
            " {% if dag_run.conf.get('competition') %}--competition {{ dag_run.conf['competition'] }}{% endif %}"
            " {% if dag_run.conf.get('apply') %}--apply{% endif %}"
            " {% if dag_run.conf.get('allow_chain') %}--allow-chain{% endif %}"
            " {% if dag_run.conf.get('remeasure') %}--remeasure{% endif %}"
        ),
        environment=_ENV,
        image=IMAGE,
        force_pull=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        # runtime/isolate.py의 os.unshare(CLONE_NEWNET)가 CAP_SYS_ADMIN을 요구 —
        # tune DAG는 in-process라 없지만 이 백필은 eval_isolated를 탄다.
        cap_add=["SYS_ADMIN"],
        auto_remove="success",
        mount_tmp_dir=False,
        queue="big",
        cpus=1.5,
        mem_limit="5g",
        # 대회당 승격 행 수 x eval 1회(p50 350-800s, p95 2700s) + drift probe 1회.
        # 여러 대회를 한 conf로 돌리면 합이 커진다 — 보통 대회별로 트리거한다.
        execution_timeout=timedelta(hours=6),
    )


reflexion_rondo_backfill_materialized()

"""reflexion-rondo Optuna 튜닝 DAG — #230, 900s attempt CPU 예산 밖 별도 레인.

dag_run.conf: {"competition": "s4e10", "n_trials": 100, "timeout_sec": 3600}
n_trials/timeout_sec 생략 시 bin/tune_pipeline.py 기본값(100 trial, 무제한 timeout — DAG의
execution_timeout이 바깥 상한).

reflexion_rondo_cycle.py의 attempt 컨테이너와 동일한 이미지·환경을 재사용한다 —
확정 pipeline 소스를 exec하는 신뢰 경계가 attempt 평가와 같으므로(decisions.md ADR-035),
별도 이미지를 만들 이유가 없다. 수동 트리거 전용 — daemon이나 다른 DAG가 자동으로 호출하지
않는다(#230 범위: 코어 튜닝 기능 + 수동 실행 경로, 자동 스케줄링은 #233/#236에서 실측 후 결정).
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
    "PYTHONUNBUFFERED":        "1",
    "RONDO_DB_URL":            "{{ var.value.rondo_db_url }}",
    "OLLAMA_BASE_URL":         "{{ var.value.ollama_base_url }}",
    "OLLAMA_CLOUD_BASE_URL":   "{{ var.value.ollama_cloud_base_url }}",
    "OLLAMA_API_KEY":          "{{ var.value.ollama_api_key }}",
    "MINIO_ENDPOINT":          "{{ var.value.minio_endpoint }}",
}


@dag(
    dag_id="reflexion_rondo_tune",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    tags=["rondo"],
    on_failure_callback=notify_discord_on_failure,
)
def reflexion_rondo_tune() -> None:
    DockerOperator(
        task_id="tune",
        command=(
            "uv run --no-sync python -m bin.tune_pipeline"
            " --competition {{ dag_run.conf['competition'] }}"
            " --n-trials {{ dag_run.conf.get('n_trials', 100) }}"
            " {% if dag_run.conf.get('timeout_sec') %}--timeout-sec {{ dag_run.conf['timeout_sec'] }}{% endif %}"
        ),
        environment=_ENV,
        image=IMAGE,
        force_pull=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        auto_remove="success",
        mount_tmp_dir=False,
        # attempt_i(reflexion_rondo_cycle.py)와 동일 queue="big" — 확정 pipeline 재학습이
        # LightGBM/XGBoost/CatBoost 등 CPU 바운드 fit을 수백 trial 반복하므로 attempt와
        # 같은 등급의 컴퓨트가 필요하다. cpus는 attempt보다 여유 있게(튜닝은 병렬 attempt
        # 3개와 경합할 필요가 없는 단독 실행이 보통이므로).
        queue="big",
        cpus=2.0,
        # bin/tune_pipeline.py는 runtime/isolate.py의 RSS 워치독을 거치지 않는다(ADR-035
        # 트레이드오프 — trial마다 subprocess를 새로 띄우면 튜닝의 효율 이점이 사라져
        # in-process로 돈다) — 그 워치독이 없는 대신 컨테이너 mem_limit이 유일한
        # 메모리 백스톱이다. attempt(5g, reflexion-rondo#154/#162)보다 여유 있게(대회
        # 데이터가 최대 70만 행급) — reflexion-rondo#31과 동일한 이유의 백스톱.
        mem_limit="6g",
        # attempt(900s)와 달리 여기는 그 예산 밖이 핵심 요구사항(#230 배경) — n_trials가
        # 크거나 데이터가 크면 수십 분~수 시간 걸릴 수 있다. execution_timeout이 진짜
        # 바깥 상한(bin/tune_pipeline.py --timeout-sec는 멤버/모델 1개당 상한이라 ensemble
        # 멤버 여러 개면 그 합만큼 걸릴 수 있음 — 호출 시 n_trials/timeout_sec를 대회
        # 데이터 크기에 맞게 신중히 설정할 것).
        execution_timeout=timedelta(hours=4),
    )


reflexion_rondo_tune()

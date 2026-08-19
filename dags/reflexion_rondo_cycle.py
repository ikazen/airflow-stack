"""reflexion-rondo super-cycle DAG — BON-110.

DAG conf: {competition_id, stage, queue_id}
1 DAG run = 1 super-cycle:
  retrieve → [attempt_0, attempt_1, attempt_2]  (leaf, 취소하지 않고 45분까지 그대로 돔)
  retrieve → attempt_gate → promote

daemon이 큐에서 아이템을 꺼내 trigger하고, promote task 완료 후 DB에서 결과를 읽는다
(reflexion-rondo#204 — daemon은 더 이상 DAG run 전체가 아니라 promote 하나만 기다린다).
시크릿은 Airflow Variable로 주입 (var.value.xxx) — .env 마운트 없음.

BON-237: 모든 task에 `--run-id {{ run_id }}` 전달 — raw.super_cycle_context의
조회 키. queue_id는 같은 super-cycle의 여러 cycle(dag run)이 공유해서
(max_active_runs=4) 동시 실행 시 서로의 context row를 덮어쓰는 레이스가 있었다.
run_id(Airflow dag_run_id)는 cycle마다 유일해서 안전하다.

reflexion-rondo#203: attempt_gate가 promote를 attempt_0/1/2 3개 전부가 아니라
raw.attempts에 2개 이상 쌓일 때(또는 grace/max_wait 데드라인)까지만 기다리게 한다 —
2026-08 실측으로 가장 늦게 끝나는 attempt는 승격률 25.9%(균등확률보다 낮음)·에러율
41.6%로 대기가 대부분 실패를 기다리는 것이었다. attempt_i 자신은 취소하지 않는다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator as _DockerBase
from lib.alert import notify_discord_on_failure


class DockerOperator(_DockerBase):
    template_fields = ("command", "environment", "image")


# issue #2: 태그의 source of truth가 git(이 파일)에서 Airflow Variable로 이동.
# reflexion_rondo_deploy DAG가 빌드 후 Variable을 bump한다 — git push 불필요,
# 즉시 반영. 최초 배포 전 `rondo_task_image_version`을 현재 라이브 태그로 1회
# 수동 설정해야 한다(비어있으면 이미지 참조가 깨짐).
IMAGE = "registry.internal:5000/reflexion-rondo/task:{{ var.value.rondo_task_image_version }}"

_DOCKER_BASE = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    # os.unshare(CLONE_NEWNET) 이 CLONE_NEWNET namespace 생성에 CAP_SYS_ADMIN 을 요구.
    # network_mode="host" 는 컨테이너 자체 네트워크(DB/MinIO/Ollama) 용 — 차단은 subprocess preexec_fn 레벨.
    cap_add=["SYS_ADMIN"],
    auto_remove="success",
    mount_tmp_dir=False,
)

_DOCKER_LIGHT = dict(**_DOCKER_BASE, queue="default", cpus=0.5)
# mem_limit: reflexion-rondo/runtime/isolate.py의 RSS 워치독(EVAL_RSS_LIMIT_BYTES
# 기본 4GiB)이 1차 방어선이고 이건 백스톱이다 — 워치독이 폴링 주기(2초) 사이에
# 놓친 급격한 단일 allocation이 있어도 kill 범위를 이 컨테이너로 한정해 같은
# 호스트의 omnigent/Postgres 등 다른 프로세스가 커널 OOM killer에 말려드는 걸
# 막는다(2026-08 실측: rc=-9가 계산시간의 37%를 태움, 백스톱 부재가 원인 중 하나).
# 워치독 한도보다 높게 잡아야 워치독이 먼저 죽여 원인이 명시된 에러를 남긴다.
_DOCKER_HEAVY = dict(**_DOCKER_BASE, queue="big", cpus=1.5, mem_limit="5g")

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
    on_failure_callback=notify_discord_on_failure,
)
def reflexion_rondo_cycle() -> None:
    retrieve = DockerOperator(
        task_id="retrieve",
        command=(
            "uv run --no-sync python -m bin.run_retrieve_task"
            " --competition {{ dag_run.conf['competition_id'] }}"
            " --stage {{ dag_run.conf['stage'] }}"
            " --queue-id {{ dag_run.conf['queue_id'] }}"
            " --run-id {{ run_id }}"
        ),
        environment=_ENV,
        execution_timeout=timedelta(minutes=15),
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
                " --run-id {{ run_id }}"
                f" --attempt-index {i}"
            ),
            environment=_ENV,
            execution_timeout=timedelta(minutes=45),
            **_DOCKER_HEAVY,
        )
        for i in range(3)
    ]

    attempt_gate = DockerOperator(
        task_id="attempt_gate",
        command=(
            "uv run --no-sync python -m bin.run_attempt_gate_task"
            " --run-id {{ run_id }}"
            " --expected 3 --min-done 2"
        ),
        environment={
            **_ENV,
            "RONDO_GATE_GRACE_SEC":    "{{ var.value.get('rondo_gate_grace_sec', '900') }}",
            "RONDO_GATE_MAX_WAIT_SEC": "{{ var.value.get('rondo_gate_max_wait_sec', '3000') }}",
        },
        # attempt execution_timeout(45분=2700s)보다 위 — gate 자신의 max_wait(기본
        # 3000s=50분)를 절대 못 채우고 컨테이너가 먼저 죽는 일이 없게 여유를 둔다.
        execution_timeout=timedelta(minutes=55),
        retries=1,
        **_DOCKER_LIGHT,
    )

    promote = DockerOperator(
        task_id="promote",
        command=(
            "uv run --no-sync python -m bin.run_promote_task"
            " --queue-id {{ dag_run.conf['queue_id'] }}"
            " --run-id {{ run_id }}"
            " --competition {{ dag_run.conf['competition_id'] }}"
        ),
        environment=_ENV,
        # BON-257: PROMOTE_CONFIRM_SEEDS 3개(BON-247로 42 제거 후) × baseline+candidate
        # 2회 eval = 6회 + holdout 1회 + merge-verify 1회(BON-256) = 최대 8회
        # eval_isolated 호출. reflexion-rondo #166/#167/#168이 confirm 게이트에
        # negative memo + baseline 캐시를 추가해 평균 소요는 크게 줄었지만(실측
        # p95 11.8m 이하로 목표), 캐시는 best-effort(TTL 만료·best_source 변경 시
        # miss)라 이론상 worst-case는 그대로다. reflexion-rondo runtime/isolate.py
        # DEFAULT_TIMEOUT=1200s(이 주석은 과거 600s 기준으로 계산돼 실제값과
        # 어긋나 있었다 — issue #34) 기준 worst-case 8*1200s=9600s=160분.
        # reflect 루프/materialize 오버헤드 여유를 둬 180분으로 상향.
        execution_timeout=timedelta(minutes=180),
        trigger_rule="all_done",
        **_DOCKER_HEAVY,
    )

    retrieve >> attempts  # attempt_i는 leaf — 취소하지 않고 각자 execution_timeout까지 돈다
    retrieve >> attempt_gate >> promote


reflexion_rondo_cycle()

"""lck.pics 데이터 동기화 DAG 공통 설정.

비즈니스 로직은 private 이미지(facade)에 있고, DAG 는 `@task.docker` 로 그 이미지를
워커의 docker 소켓(DooD)으로 기동한다 (decisions L24). 태그 sha-pin 시 IMAGE 한 곳만 수정.

전제 (워커 인프라, airflow-stack tasks.md "실행 환경 격리"):
- 워커 이미지에 `apache-airflow-providers-docker` 설치
- 워커 compose 에 `/var/run/docker.sock` bind mount (DooD)
- Airflow Variable `db_url` / `db_key` 등록 (시크릿. UI 또는 `airflow variables set`)
"""
from __future__ import annotations

import pendulum

IMAGE = "registry.internal:80/lck-pics/data-sync:latest"

# 모든 동기화 task 공통. force_pull=False 는 sha-pin 시 캐시 hit (L26).
# latest 는 mac cron 병행 검증 단계 한정 — 안정 후 :<sha> 로 교체.
DOCKER_KWARGS = dict(
    image=IMAGE,
    force_pull=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    auto_remove="success",
    # DooD 에선 워커 컨테이너와 host docker 의 파일시스템이 달라 tmp mount 가 깨짐.
    mount_tmp_dir=False,
    # 시크릿은 실행 시점에 api-server 경유로 resolve (Task SDK). parse-time 노출 없음.
    # db_key 는 키 이름에 'key' 가 있어 로그에서 자동 마스킹됨.
    environment={
        "DB_URL": "{{ var.value.db_url }}",
        "DB_KEY": "{{ var.value.db_key }}",
    },
    queue="default",
)

# cron 은 start_date 의 tz 로 해석됨 (tz-aware 필수). 일일 잡 = 00:00 KST.
START_DATE = pendulum.datetime(2026, 1, 1, tz="Asia/Seoul")

TAGS = ["lck-pics", "etl"]

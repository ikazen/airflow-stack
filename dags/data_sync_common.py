"""lck.pics 데이터 동기화 DAG 공유 상수.

이미지 태그만 공유한다 — drift 가 곧 장애이기 때문 (latest -> :<sha> 핀 전환 시 한 곳만 수정).
나머지 실행 설정(env/소켓/queue)은 "실행에 필요한 내용은 DAG 에 보이게" 원칙으로 각 DAG 파일에 인라인.

전제 (워커 인프라, tasks.md "첫 도메인 워크로드"):
- 워커 이미지에 `apache-airflow-providers-docker` + 워커 compose 에 `/var/run/docker.sock` mount (DooD)
- Airflow Variable `db_url` / `db_key` 등록
"""
from __future__ import annotations

IMAGE = "registry.internal:80/lck-pics/data-sync:19a4f48"

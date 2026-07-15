from __future__ import annotations

# 이미지 태그만 공유 — 한 곳만 수정. 나머지 설정은 각 DAG 에 인라인.
#
# 태그의 source of truth가 git(이 파일)에서 Airflow Variable로 이동
# (reflexion-rondo issue #2/커밋 7b7bd6f 와 동일 패턴). 이미지 빌드+push
# (lol-list `scripts/build-and-push.sh`, M1 mac 수동)와 활성화(Variable bump)를
# 분리해, 레지스트리에 태그가 없는 채로 배포돼 pull 실패하는 사고를 방지한다.
# 최초 도입 시 현재 라이브 태그로 1회 수동 설정 필요 — 비어있으면 이미지
# 참조가 깨진다 (`airflow variables set data_sync_image_version <sha>`).
IMAGE = "registry.internal:5000/lck-pics/data-sync:{{ var.value.data_sync_image_version }}"

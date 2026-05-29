# Airflow 3 Learnings

2.x → 3.x 학습 노트. "2.x 였으면 X, 3.x 는 Y" 패턴.

현 상태: **scaffold.** Phase 3~7 진행 중 부딪힌 실제 차이를 추가.

## 핵심 차이축

### Task SDK
- 2.x: task 안에서 `Variable.get()` / `Connection.get_hook()` → 워커가 DB 직접 호출
- 3.x: 워커 DB 접근 0. Task SDK 가 api-server 경유. 자격증명 모델 근본 단순화

### Edge Executor
- 2.x: Celery + Redis broker + Flower (운영 피곤의 본체)
- 3.x: HTTPS long-poll 한 채널. broker 0. NAT 뒤 워커 자연
- provider: `apache-airflow-providers-edge3` (Airflow 3 용 — 끝에 `3`). executor: `airflow.providers.edge3.executors.EdgeExecutor`

### DAG Versioning
- 2.x: 현재 DAG 파일 = 단일 진실. 과거 run 코드 추적 어려움
- 3.x: DagRun 별 버전 박제. UI 에서 "어느 코드로 돈 run" 확인 — DAG Bundle 위에서 동작 (commit pin)

### DAG Bundle (DAG 분배)
- 2.x: DAG 폴더 = 모든 컴포넌트가 같은 파일시스템 봐야 함. self-host 면 호스트마다 git pull / NFS / rsync 수동 분배
- 3.x: **bundle** 이 분배 추상화. `LocalDagBundle` (폴더) 또는 `GitDagBundle` (repo 직접 fetch). dag-processor 가 파싱, **워커도 task 실행 시 bundle 에서 materialize** — 워커가 DAG 코드 가져야 한다는 사실은 불변 (executor 종류 무관), bundle 은 그 분배를 자동화
- managed (MWAA/Composer) 의 "버킷에 올리면 끝" = 플랫폼이 깐 bundle/분배 계층. self-host 에선 GitDagBundle 이 그 역할 (`decisions.md` L27)
- `GitDagBundle` public repo: `repo_url` 직접 전달, `git_conn_id` 생략 (0.2.3+ public 지원). config = `[dag_processor] dag_bundle_config_list` JSON

### DAG Processor 분리 (L15)
- 2.x: scheduler 안 DAG bag → 파싱 오류가 scheduler 영향
- 3.x: 별도 프로세스. 권장 모델

### Data Assets
- 2.x: Dataset (2.4+) 의 진화형
- 3.x: `@asset` 으로 fully realized. 전통 `@dag` 와 선택 기준은 CLAUDE.md "워크로드 모델링" 섹션 (도그마 없음)
- Dagster SDA 와 비교: paradigm 깊이는 Dagster 가 정제, Airflow 는 bolted-on

### `@dag` 작성 gotcha
- `@dag` 함수는 모듈 끝에서 호출(`my_dag()`)해야 dag-processor 가 발견
- cron 은 `start_date` 의 timezone 에서 해석 — KST 의도면 tz-aware `start_date` (또는 `CronTriggerTimetable(timezone=...)`). `@dag` 에 timezone 인자 없음
- `from <pkg> import ...` 는 `@task` 콜러블 안에서 (파싱 시점 실행 안 됨, 워커 실행 시점에만 import) → dag-processor 는 도메인 deps 불필요
- 외부 호출 task 는 `retries` 필수 (transient 대비). 특히 `queue="gpu"` 는 M1 intermittent → retry 없으면 sleep 중 실패

### Auth Manager
- 2.x: FAB RBAC 가 사실상 기본
- 3.x: `SimpleAuthManager` 가 기본. 단 공식 문서가 dev/test 전용·production 비권장 명시 (`decisions.md` R3). FAB 는 `FabAuthManager` 로 분리
- 경로: `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`. 2FA·RBAC 없음

### Constraints
- 3.2.x 라인: `-c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt`
- provider 들이 빠르게 다듬어지는 중 → 핀 필수

## 운영 중 추가할 항목

(실제 발견 시 채움 — troubleshooting.md 의 사례와 별개로, 신기능 학습 노트만)

- Edge Worker 의 registration / heartbeat 동작 (UI 디테일)
- DAG Versioning 의 retention (old version store size)
- M1 sleep/wake 시 edge worker 재연결 latency
- `EdgeExecutor` import path 확인 완료 (2026-05): provider `apache-airflow-providers-edge3`, executor `airflow.providers.edge3.executors.EdgeExecutor`

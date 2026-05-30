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

### 메트릭 (StatsD) — 어느 프로세스가 emit 하나
- 이미지에 `statsd` 클라이언트 필요 (공식 image 미포함). `[metrics] statsd_on/host/port/prefix`. 태그 메트릭은 `statsd_influxdb_enabled=True` → statsd-exporter 가 influx 태그를 라벨로 파싱.
- **함정: api-server(uvicorn) 프로세스는 metrics 클라이언트를 init 하지 않음.** scheduler / dag-processor / triggerer / edge-worker(장기 실행 job)만 emit. → api-server 요청 핸들러 안에서 부르는 `Stats`(edge3 의 `edge_worker.*`: set_state heartbeat 에서 emit)는 statsd 로 안 나감. 네트워크/influx 파싱은 정상인데(raw UDP 는 도달) Airflow Stats 만 무음.
- 결과: `edge_worker.connected` 기반 "워커 오프라인" 알람 불가. 게다가 scheduler 의 liveness 가 UNKNOWN 시 `connected=0` 만 찍고 복구 시 1 을 안 찍어 알람이 안 풀림. → 워커 오프라인은 `node_exporter` `up`(NodeDown, mac 포함)으로 대체.
- 잘 나오는 것(scheduler/dag-processor 발): `scheduler_heartbeat`, `dag_processing.import_errors`, `dagrun.duration.failed.<dag_id>` 등 → 알람은 이걸로.

## Edge worker sleep/wake (intermittent 노트북 워커)

mac-server 는 클램쉘 노트북 → 자주 sleep. "sleep 중 돌던 task 와 worker 가 깨어나면 어떻게 되나" 를 파고든 기록. (운영 절차는 `troubleshooting.md`/`setup.md`, 여긴 "왜 그렇게 동작하는가")

### 전제 — 살아있음의 증거는 heartbeat 2종

- **worker heartbeat**: worker 가 주기적으로 중앙에 "살아있음 + job N개 실행중" 보고 → DB `edge_worker.last_update` 갱신. 주기 = `edge.heartbeat_interval` (기본 30s).
- **task heartbeat**: 실행 중 task supervisor 가 보고 → `task_instance.last_heartbeat_at` 갱신.

sleep = clock freeze → 둘 다 멈춤 → 깨어나면 stale. 이 stale 을 중앙이 감지해 복구하는 게 전부.

### 중앙의 자동 복구 3종 (edge3 기본, 설정 불필요)

1. **worker liveness** (`EdgeExecutor._check_worker_liveness`): `last_update` 가 `heartbeat_interval × 5` (= **150s**) 넘게 stale → worker state `UNKNOWN`. UI 에서 보이는 "unknown" 이 이것.
2. **orphaned job reconcile** (`_update_orphaned_jobs`): RUNNING `edge_job` 의 last_update 가 `scheduler.task_instance_heartbeat_timeout` (= **300s**, 기본) 넘으면 job.state 를 TI 실제 state 에 맞춤. **단 TI 를 직접 fail 시키진 않음** — TI 를 따라갈 뿐.
3. **TI heartbeat timeout** (scheduler): TI 의 `last_heartbeat_at` 가 300s 넘게 stale → 로그 "Failing TIs without heartbeat" → TI `failed` → retries 있으면 재시도. **실제로 좀비 task 를 끝내는 주체가 이것.**

> 2.x `scheduler_zombie_task_threshold` 는 3.x 에서 `task_instance_heartbeat_timeout` 으로 개명. 옛 키 / `task_heartbeat_sec` 는 무효 (3.x 에선 이미 기본 300).

### worker 재등록 규칙 (409 의 정체)

`POST /worker/{name}` (register) 는 기존 row state 가 `OFFLINE`/`UNKNOWN`/`OFFLINE_MAINTENANCE` 일 때만 재사용 허용. 그 외(`running`/`idle`/`terminating`/`starting`)면 **409 "already active"** — 동명 워커 중복 방지.

### worker 의 graceful shutdown(drain) 루프

main loop = `while not self.drain or self.jobs:` — drain 신호를 받아도 **job 이 남아있으면 루프를 못 빠져나감**. 루프를 나가야 그 뒤 `worker_set_state(OFFLINE)` 에 도달.
- SIGTERM(=`docker stop`) → drain 시작, job 들에 SIGTERM.
- `edge.drain_timeout_sec` 기본 **0 = 무한 대기**: job 이 끝날 때까지 기다림.

### 무슨 일이 터졌나

sleep 중 task 실행 → 깨어남 → 두 갈래로 깨짐:

**(A) 좀비 task**: clock jump 로 task 의 JWT 만료 → 결과 보고 실패. worker 는 phantom 슬롯을 쥔 채 "N still running" 만 반복. → 위 #3(TI heartbeat timeout)이 ~5분에 정리.

**(B) worker wedge + crash loop (더 고약)**: 손으로 복구하려 worker 를 재시작하면 —
- `restart: unless-stopped` 가 worker 를 계속 재기동 → register → 기존 row 가 아직 `running`/`terminating`(150s 미경과) → 409 → 죽음 → 또 재기동 …
- 재시작이 `last_update` 를 계속 건드려 **150s 를 영영 못 채우는 self-inflicted crash loop**.
- 게다가 in-flight task 가 있으면 force-recreate 시 worker 가 `terminating`(drain) 진입 → `drain_timeout=0` 이라 job 안 끝나 루프 못 나감 → docker 가 (기본 10s 후) SIGKILL → `OFFLINE` 못 찍고 `terminating` 에 박제 → 새 worker 영원히 409.

교훈: **가만히 두면** 150s 후 UNKNOWN 되어 스스로 재등록 가능. `docker restart` 연타가 오히려 망가뜨림. 하지만 노트북 깨울 때마다 5분 기다릴 순 없으니 ↓.

### 해결 (3겹)

1. **wake 훅** (`sleepwatcher` + `~/.wakeup`): 깨어날 때 자동 `docker compose up -d --force-recreate edge-worker`. 사람 개입 제거.
2. **drain 빠른 종료** — (B)의 핵심: `AIRFLOW__EDGE__DRAIN_TIMEOUT_SEC=10` + `DRAIN_KILL_GRACE_SEC=5` + compose `stop_grace_period: 30s`. force-recreate 의 SIGTERM → drain 10s → 남은 좀비 job SIGTERM, 5s 뒤 SIGKILL → job 비워짐 → 루프 탈출 → **`OFFLINE` 깨끗이 찍고 종료** → 새 worker 가 register 할 때 기존 row 가 OFFLINE → 즉시 허용 → idle. crash loop·150s 대기 소멸.
   - `stop_grace_period` 가 drain 총시간(10+5)보다 짧으면 docker 가 먼저 SIGKILL → 다시 terminating 박제. 그래서 grace 를 길게(30s).
3. **retries** (DAG): reconcile 된 좀비 task 가 재실행되도록. 손실 치명적 DAG(`daily_meta`)만 — 잦은 DAG(`sync_*`)는 다음 발화가 흡수.

### 검증 결과 (2026-05-31)

- **idle** sleep/wake: worker ~20s 자동 복귀.
- **in-flight task** sleep/wake: worker ~1분(wake 훅+drain) 깨끗이 idle, task 는 `last_heartbeat_at + 300s` 에 `failed`(retry 있으면 재실행). 둘 다 무인.

### `@task.docker` 추가 주의

`@task.docker` task 는 worker 컨테이너가 아니라 **별도 DooD 컨테이너**에서 실행. worker 를 force-recreate 해도 그 task 컨테이너는 orphan 으로 남음 — 중앙 reconcile 이 job/TI 는 정리하지만 컨테이너 자체는 abnormal exit 시 잔류 가능 (`auto_remove="success"` 라 성공 종료시만 자동 삭제). 또한 task 가 로직을 끝내고도 client 미종료(httpx/supabase) 로 프로세스가 exit 못 하고 hang 하면 슬롯 점유 → `execution_timeout` 으로 가드 (초과 시 DockerOperator on_kill 이 컨테이너 stop).


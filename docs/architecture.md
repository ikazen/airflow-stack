# Architecture

Airflow 3.2.x workload. Edge Executor 기반. 인프라 (호스트·네트워크·OCI·Caddy·Postgres·Registry) 는 별도 repo `nexus-prime`.

## 서비스 배치

| 호스트 | airflow 서비스 |
|---|---|
| ops-vm | api-server / scheduler / dag-processor / triggerer / edge-worker-ops (`ops` c=2) |
| worker-vm | edge-worker-default (`default` c=2, `--edge-hostname worker-vm`) / edge-worker-big (`big` c=1, `--edge-hostname worker-vm-big`) |
| mac-server | edge-worker-default (`default` c=8, `--edge-hostname mac-server`) / edge-worker-big (`gpu,big` c=4, `--edge-hostname mac-server-big`) |

한 노드에 두 워커 (T-shirt sizing, `decisions.md` R5). edge3 concurrency 는 워커 단위 단일 값이라 사이즈별 cap 차등은 워커 프로세스 분리로만. `network_mode: host` 라 hostname 지시어 충돌 → `--edge-hostname` 으로 워커 이름 구분.

호스트·네트워크 토폴로지·OCI 자원·Tailscale 메쉬는 `nexus-prime:docs/architecture.md`.

## Edge Executor 의 핵심

- 워커 → ops-vm = **HTTPS long-poll 한 채널만**. DB / Redis 접근 0
- Task SDK 가 모든 상태를 api-server 경유
- NAT 뒤 M1 도 outbound 만으로 동작 (broker 없음)
- JWT: 공유 `AIRFLOW__API_AUTH__JWT_SECRET` (HMAC). 워커가 자체 서명한 토큰 제시 — api-server 발행 토큰 아님

## Queue

| queue | 구독 worker | concurrency | 라우팅 |
|---|---|---|---|
| `default` | worker-vm / mac-server | vm=2, mac=8 | 미지정 task 의 기본 |
| `big` | worker-vm-big / mac-server-big | vm=1, mac=4 | heavy task 명시 opt-in (`queue="big"`) |
| `gpu` | mac-server-big (`gpu,big` 공동 구독) | 4 (big 공유) | `queue="gpu"` 명시. GPU / Neural Engine. M1 가용성 가정 금지 |
| `ops` | ops-vm | 2 | control-plane 작업 전용 (ops-vm 은 `default` 미구독) |

cap = admission(슬롯 수). 실제 리소스 상한은 `@task.docker` `mem_limit`/`cpus` 로 별도 세트.

## DAG 목록

### lck-pics 데이터 동기화

| DAG | 스케줄 | queue | 비고 |
|---|---|---|---|
| `sync_matches` | `*/10 * * * *` | default | `max_active_runs=1`, `exec_timeout=5m` |
| `sync_secondary` | `*/15 * * * *` | default | `max_active_runs=1`, `exec_timeout=5m` |
| `daily_meta` | `0 0 * * *` (KST) | default | `retries=2`, `exec_timeout=10m`. leagues → [matches, secondary] → report |

세 DAG 모두 `dags/data_sync_common.py` 의 `IMAGE` 공유 (sha-pinned). Airflow Variables `db_url`·`db_key` 로 자격증명 주입.

### reflexion-rondo

| DAG | 스케줄 | 비고 |
|---|---|---|
| `reflexion_rondo_cycle` | `None` (daemon이 trigger) | `max_active_runs=4`, `conf={competition_id, stage, queue_id}` |

retrieve(default) → attempt_0/1/2(big) → promote(default). Airflow Variables 로 자격증명 주입 (`rondo_db_url` 등). `network_mode="host"`.

### 운영

| DAG | 스케줄 | queue | 비고 |
|---|---|---|---|
| `maintenance` | `0 6 * * 3` (KST) | ops | 로그 14일 보존. 로그 볼륨이 ops-vm 에 있어 `queue="ops"` 필수 |
| `test_environment` | None (수동) | ops/default/gpu | 3 노드 환경 확인 |

## `@task.docker` DooD 패턴

워크로드 task 는 DooD(Docker-out-of-Docker) 컨테이너에서 실행. 공통 설정:

```python
docker_url="unix://var/run/docker.sock"
network_mode="bridge"          # rondo 는 "host"
auto_remove="success"          # 비정상 종료 시 컨테이너 잔류 가능
mount_tmp_dir=False            # 워커-host 파일시스템이 달라 tmp mount 깨짐
force_pull=False               # sha-pinned 이미지 캐시 hit
```

- task-sdk 를 task image 에 넣지 말 것 — DooD 환경에서 comms reinit fd 에러로 즉사
- `execution_timeout` 필수 — httpx/supabase 미종료 시 프로세스 hang 방지

## DAG processor 분리 (L15)

scheduler 와 다른 컨테이너로 격리. 파싱 오류가 scheduler 에 영향 안 줌. Airflow 3 권장.

## DAG 분배 = GitDagBundle (L27)

DAG 파일은 호스트 bind mount 가 아니라 **Airflow 가 repo 에서 직접 fetch**. dag-processor (파싱) 와 워커 (실행 시 materialize) 가 각자 `apache-airflow-providers-git` 의 `GitDagBundle` 로 `dags/` 를 당겨옴. public repo 라 `repo_url` 직접 — connection 없음.

- 배포 = repo 에 `git push`. 각 컴포넌트가 `refresh_interval` (60s) 마다 자동 갱신 — 호스트 per-node `git pull` 불필요
- DagRun 마다 commit pin → DAG Versioning UI
- 설정: `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` (3 호스트 `.env` 동일)
- 단, compose / Dockerfile / `.env` 변경은 bundle 밖 — 여전히 호스트 `git pull` + 재배포 필요

## 이미지 (L19)

api-server·scheduler·dag-processor·워커 모두 공식 `apache/airflow:3.2.x` 확장 커스텀 이미지 — 공식 이미지엔 없는 edge3 + git provider (GitDagBundle 용) 포함. 빌드는 `docs/setup.md`.

워크로드 task 실행은 `@task.docker` 별도 image (표준) — 도메인 deps 는 거기로, airflow 이미지는 thin. `decisions.md` L24/L26.

## 인프라 의존 (nexus-prime 제공)

서비스 입장에서 본 결합점·내부 주소·발급 절차는 `nexus-prime:docs/dev-guide.md` 가 정본.

- `nexus` docker network (external) — airflow 서비스가 join (`networks: { nexus: { external: true } }`)
- `postgres:5432` — 공유 DB 안 `airflow` database (DB/user 발급은 dev-guide)
- `registry.internal` — `@task.docker` image registry. DNS 미작동 시 fallback `<ops-vm-tailnet>:5000`
- 워커 → api-server = Tailscale 직결 (cert 불필요, edge API 공인 노출 X)

airflow 는 nexus-prime 의 `compose/_hosts/` include 가 아니라 **별도 repo 의 자체 compose** 로 외부 `nexus` 네트워크에 join — dev-guide 의 "신규 서비스 추가 체크리스트" (nexus-prime 내부 서비스용) 와 구분.

# CLAUDE.md

Airflow 3.2.x self-host. Claude 세션 컨벤션. 사용자 글로벌 `~/.claude/CLAUDE.md` 위에 얹힘.

---

## 공개 repo 정책 (최우선)

코드·문서·커밋·주석 어디서도 평문 노출 금지: 사용자 도메인 / 옛 서브도메인 / 옛 repo 식별자 (한글·영문 변종) / 공인 IP / 본인 IP / tailnet 실제 이름 / 사용자 home 경로. 전부 placeholder (`<your-domain>`, `<previous-repo>`, `<tailnet>.ts.net`, ...).

예외: RFC1918 사설 IP (`10.0.0.0/16` 등) 만 OK.

정확한 옛 식별자 grep 목록은 **repo 외 메모리 보관** — 정책 문서가 자기 정책 안 어기게. 새 파일 작성 / 수정 후 메모리(`public-repo-scrub-grep-list`)의 패턴으로 `git grep` 검증, 매치 0 이어야 함.

사용자가 알려주는 실제 값은 받아 적지 말고 placeholder 로만. 실제 값은 `.env` / 로컬 secrets / Claude 메모리에만.

---

## 인프라 배치

인프라 layer (호스트·Tailscale·Caddy·Postgres·Registry) 는 별도 repo `nexus-prime`. 이 repo 는 nexus docker network 에 join 하는 외부 서비스.

| 호스트 | 역할 | compose 파일 |
|---|---|---|
| ops-vm | api-server / scheduler / dag-processor / triggerer / edge-worker-ops(`ops-vm` c=2, docker.sock 마운트) | `infra/ops-vm/docker-compose.yml` |
| worker-vm | edge-worker-default(`default` c=2) / edge-worker-big(`big` c=1) | `infra/worker-vm/docker-compose.yml` |
| mac-server | edge-worker-default(`default` c=8) / edge-worker-big(`gpu,big` c=4) | `infra/mac-server/docker-compose.yml` |

- ops-vm edge-worker 는 같은 compose 안에서 api-server 에 내부 주소로 직결: `http://api-server:8080/edge_worker/v1/rpcapi`
- worker-vm / mac-server 는 Tailscale 직결 (`AIRFLOW__EDGE__API_URL` = `http://<oci-ops-tailnet-ip>:8080/edge_worker/v1/rpcapi`)
- api-server 는 ops-vm Tailscale IP 로만 포트 바인드 (`${OPS_TAILNET_IP}:8080:8080`) — 공인 노출 없음

### 이미지

`infra/airflow.Dockerfile` 기준 (`apache/airflow:3.2.1`):
```
apache-airflow-providers-edge3==3.6.0
apache-airflow-providers-git==0.3.1
apache-airflow-providers-docker==4.5.5
statsd==4.0.1
```
provider 핀 변경 = `--build` 필수.

### mac-server 특이사항

Colima VM 사용. `docker.sock` 경로 = `/var/run/docker.sock` (Colima default). `DOCKER_GID` = Colima VM 내 docker gid (`colima ssh -- getent group docker | cut -d: -f3`).

drain 설정이 compose에 박혀 있음:
```yaml
environment:
  AIRFLOW__EDGE__DRAIN_TIMEOUT_SEC: "10"
  AIRFLOW__EDGE__DRAIN_KILL_GRACE_SEC: "5"
stop_grace_period: 30s
```
이 값들은 sleep/wake 후 `force-recreate` 시 빠른 OFFLINE 등록 보장. 건드리지 말 것.

---

## Queue

T-shirt sizing. edge3 concurrency 는 워커 단위 단일 값 (per-queue 설정 없음) → 사이즈별 cap 차등은 워커 프로세스 분리로만. cap = admission(슬롯 수), 실제 리소스 상한은 `@task.docker` `mem_limit`/`cpus` 로 세트.

| queue | 구독 worker | concurrency | 비고 |
|---|---|---|---|
| `default` | worker-vm-default, mac-server-default | vm=2, mac=8 | |
| `big` | worker-vm-big, mac-server-big | vm=1, mac=4(gpu공유) | |
| `gpu` | mac-server-big(`gpu,big` 공동 구독) | 4(big 공유) | |
| `ops-vm` | ops-vm edge-worker-ops | 2 | privileged 인프라 유지보수 전용 (docker.sock 마운트 — 일반 워크로드 라우팅 금지) |
| `worker-vm` | worker-vm-default(공동 구독) | 2(default 공유) | worker-vm 전용 docker prune. 호스트 타겟 보장용 전용 큐 |
| `mac-server` | mac-server-default(공동 구독) | 8(default 공유) | mac-server 전용 docker prune. 호스트 타겟 보장용 전용 큐 |

`--edge-hostname` 으로 워커 이름 구분 (한 노드 두 워커 → 이름 충돌 방지):
- worker-vm: `worker-vm` / `worker-vm-big`
- mac-server: `mac-server` / `mac-server-big`

---

## 코드 배포 (2층 모델)

### DAG 파일 (`dags/`)

`git push` = 배포. GitDagBundle 이 60초마다 repo 에서 `dags/` 를 fetch. 호스트 pull 불필요.

bundle config key (`AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`):
```json
[{"name":"airflow-stack","classpath":"airflow.providers.git.bundles.git.GitDagBundle",
  "kwargs":{"repo_url":"https://github.com/<your-repo>/airflow-stack.git",
            "tracking_ref":"main","subdir":"dags","refresh_interval":60}}]
```
3 호스트 `.env` 에 동일 값. dag-processor(파싱)·워커(task 실행 시 materialize) 모두 이 bundle 에서 당겨옴.

### task body (`@task.docker` image)

1. `registry.internal:5000/<namespace>/<name>:<sha>` 로 빌드·push
2. DAG 의 image 태그(sha) 를 새 값으로 갱신 → `git push`
3. 워커가 다음 task 실행 시 `force_pull=False` 로 sha-pinned 이미지 사용 (노드 당 sha 당 1회 pull)

image sha 변경 단일 위치:
- `dags/data_sync_common.py` — `IMAGE = "registry.internal:5000/..."` (sync 3종 DAG 공유)
- `dags/reflexion_rondo_cycle.py` — `IMAGE = "registry.internal:5000/..."` (인라인)

### 인프라 변경 (compose / Dockerfile / `.env`)

bundle 밖 → 해당 호스트 `git pull` + `docker compose -f infra/<host>/docker-compose.yml up -d [--build]`

---

## DAG 목록

### 도메인 워크로드 (lck-pics 데이터 동기화)

| DAG | 스케줄 | queue | retries | 비고 |
|---|---|---|---|---|
| `sync_matches` | `*/10 * * * *` | default | 없음 | `max_active_runs=1`, `exec_timeout=5m` |
| `sync_secondary` | `*/15 * * * *` | default | 없음 | `max_active_runs=1`, `exec_timeout=5m` |
| `daily_meta` | `0 0 * * *` (KST) | default | retries=2, delay=1m, exec_timeout=10m | leagues → [matches, secondary] → report 순서 의존. retries 필수 (mac sleep 대비) |

세 DAG 모두 `data_sync_common.py` 의 `IMAGE` 공유. `environment` 는 Airflow Variable 템플릿 (`{{ var.value.db_url }}`, `{{ var.value.db_key }}`).

`@task.docker` 공통 설정:
```python
docker_url="unix://var/run/docker.sock"   # DooD
network_mode="bridge"
auto_remove="success"                      # 성공 종료 시만 컨테이너 자동 삭제
mount_tmp_dir=False                        # DooD: 워커-host 파일시스템 달라 tmp mount 깨짐
force_pull=False
```

### 도메인 워크로드 (reflexion-rondo)

| DAG | 스케줄 | 비고 |
|---|---|---|
| `reflexion_rondo_cycle` | `schedule=None` (daemon이 trigger) | `max_active_runs=4` |
| `reflexion_rondo_autosubmit` | `0 6 * * *` (KST 06:00) | ops-vm 큐 단일 task, `max_active_runs=1` |
| `reflexion_rondo_deploy` | `schedule=None` (수동, `{"tag": "vX.Y.Z"}`) | daemon+task 이미지 빌드+push+사전검증+task Variable bump. `ops-vm` 큐 |

`reflexion_rondo_cycle`: `conf` 주입 `{competition_id, stage, queue_id}`. 태스크: `retrieve`(default) → `attempt_0/1/2`(big) → `promote`(default).
`network_mode="host"` (lck-pics 와 달리 bridge 아님).
시크릿은 `.env` 마운트 없이 Airflow Variable 로 주입: `rondo_db_url`, `ollama_base_url`, `ollama_cloud_base_url`, `ollama_api_key`, `minio_endpoint`.
task 이미지 태그도 Variable(`rondo_task_image_version`) — git 하드코딩 아님(issue #2, decisions L29). `reflexion_rondo_deploy` DAG가 빌드 직후 bump.

`reflexion_rondo_autosubmit`: 최근 24h cycle 실행 대회 중 best CV 개선 시에만 Kaggle 자동 제출.
daemon `POST /api/submissions/auto` 를 HTTP 호출 (Docker 없음 — `http://rondo-daemon:8000` nexus 서비스명 직결).

`reflexion_rondo_deploy`: 수동 트리거(`{"tag": "vX.Y.Z"}`), daemon+task 이미지 빌드+push+사전검증 후 task Variable bump. `ops-vm` 큐 docker.sock 재사용(`dags/lib/image_deploy.py` 공용 헬퍼) — 신규 credential 불필요(public repo clone 무인증, registry 무인증).

DockerOperator 를 직접 사용 (템플릿이 필요해 `@task.docker` 대신):
```python
class DockerOperator(_DockerBase):
    template_fields = ("command", "environment", "image")   # image: Variable 기반 태그 템플릿용
```

### 운영 DAG

| DAG | 스케줄 | queue | 비고 |
|---|---|---|---|
| `maint_airflow` | `0 6 * * 3` (수요일 KST 06:00) | ops-vm | 로그 볼륨이 ops-vm 에 있어 반드시 `queue="ops-vm"`. db clean 자동화 포함 |
| `maint_registry` | `0 4 * * *` (매일 KST 04:00) | ops-vm/worker-vm/mac-server | docker.sock DooD — registry retention+GC+build cache prune(ops-vm) + 노드별 미사용 이미지+build cache prune(worker-vm/mac-server, 168h 보존). 노드별 task 는 서로 독립, 각자 큐에서 병렬 |
| `test_environment` | `None` (수동) | ops-vm/default/gpu | 3 노드 환경 확인용 |

`maint_airflow` 는 `LOG_DIR=/opt/airflow/logs` 하위 `.log` 파일 14일 초과분 삭제 + `db clean`(DooD exec 로 scheduler 컨테이너에서 실행, 14일 이전 메타 삭제) — 수동 fallback 절차는 `runbook.md` 참조.

---

## Airflow Variables (현재 사용 중)

변수명만 기록. 실제 값은 `.env` / Bitwarden.

| key | 용도 | DAG |
|---|---|---|
| `db_url` | lck-pics Supabase URL | sync_matches, sync_secondary, daily_meta |
| `db_key` | lck-pics Supabase service role key | sync_matches, sync_secondary, daily_meta |
| `data_sync_image_version` | lck-pics sync task 이미지 태그 | sync_matches, sync_secondary, daily_meta |
| `rondo_db_url` | reflexion-rondo DB | reflexion_rondo_cycle |
| `ollama_base_url` | Ollama API 엔드포인트 | reflexion_rondo_cycle |
| `ollama_cloud_base_url` | Ollama 클라우드 API 엔드포인트 | reflexion_rondo_cycle |
| `ollama_api_key` | Ollama API key | reflexion_rondo_cycle |
| `minio_endpoint` | MinIO S3 엔드포인트 | reflexion_rondo_cycle |
| `rondo_task_image_version` | task 이미지 태그(예: `v1.2.27`). `reflexion_rondo_deploy`가 빌드 직후 bump | reflexion_rondo_cycle |

> rondo 모델명은 Airflow Variable로 주입하지 않는다 — task 이미지 `config/settings.py` 기본값이 단일 소스(평문, 비밀 아님).

---

## Edge Executor 사고

워커는 DB 안 침. Task SDK 가 모든 상태를 api-server 경유. 2.x 의 "task 안 `Variable.get()` → DB 직접" 패턴 금지. task callable 은 모든 워커 호스트에 import 가능해야 함.

JWT: 공유 `AIRFLOW__API_AUTH__JWT_SECRET` (HMAC). 워커가 자체 서명한 토큰을 제시. (`AIRFLOW__EDGE__JWT_SECRET` 아님 — 키 이름 혼동 주의.)

---

## 워크로드 모델링 — 상황별 선택, 도그마 없음

전통 `@dag` 와 Data Asset (`@asset`) 둘 다 자유롭게. 신호로 판단:

- **전통 DAG**: 시간 cron 본질, 단일 잡, dependency 적은 ETL
- **Data Asset**: 여러 데이터의 lineage 가 운영 직관, downstream 이 dep 으로 굴러감
- **외부 polling / async sensor**: 전통 `@dag` + triggerer

Airflow 3 채택의 본 가치는 Edge Executor + Task SDK + DAG Versioning 이지 모델링 패러다임 강제가 아님.

---

## 알려진 gotcha

### DooD `@task.docker` / `DockerOperator`
- `task-sdk` 를 task image 안에 넣지 말 것 — DooD 환경에서 `comms reinit fd` 에러로 task 즉사
- `mount_tmp_dir=False` 필수 — 워커 컨테이너와 host 파일시스템이 달라 mount 깨짐
- `auto_remove="success"` — 비정상 종료 시 컨테이너 잔류 가능. 슬롯 점유 hang 방지를 위해 `execution_timeout` 세트 필수
- `network_mode="host"` 쓸 때는 포트 충돌 고려. lck-pics 는 `bridge`, rondo 는 `host`

### `@task` 안 import
`from <pkg> import ...` 는 task callable 안에서만 — 파싱 시점에 실행되지 않아 dag-processor 가 도메인 deps 불필요

### cron + timezone
`start_date` 를 tz-aware 로 지정해야 KST cron 해석 정확 (`pendulum.datetime(..., tz="Asia/Seoul")`)

### 워커 recreate 이름 충돌
`up -d` 후 빠른 recreate 시 `A worker with the name '<host>' is already active` crash-loop. 해소: 워커 stop → ops-vm 에서 `airflow edge remove-remote-edge-worker -H <hostname>` → 재시작. `troubleshooting.md` / `runbook.md` 참조.

### db clean — task 프로세스 직접 불가, DooD exec 로 자동화
Task SDK 가 task 에 `SQL_ALCHEMY_CONN` 을 주지 않아 task 프로세스 자체에서 `airflow db clean` 실행 불가. `maint_airflow` 가 docker.sock DooD exec 로 scheduler 컨테이너(`airflow-scheduler-1`)에서 실행 — `maint_registry` 의 registry GC 와 동일 패턴. 수동 필요 시에도 ops-vm scheduler 컨테이너에서 직접.

### 메트릭
api-server(uvicorn) 는 StatsD 클라이언트를 init 안 함 → `edge_worker.*` 메트릭 무음. 워커 오프라인 감지는 `node_exporter up` 으로 대체.

---

## 의존성

airflow 이미지: `apache-airflow==3.2.x` + edge3/git/docker provider 핀. 도메인 deps 는 `@task.docker` task 이미지에. constraints:
```
https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.13.txt
```

---

## 네트워크

노드 간 = Tailscale. 공인 노출 = Caddy 뒤 UI/API 한 군데. SSH 공개 폐지, 본인 IP /32 fallback. edge API 는 공인 노출 없음 (Tailscale 직결).

---

## 진입점

- 결정: `docs/decisions.md`
- 작업·진행 상태·백로그: GitHub Issues (가변 작업). 마일스톤은 GitHub Milestone
- 셋업 절차: `docs/setup.md`
- 운영 절차: `docs/runbook.md`
- 운영 문제: `docs/troubleshooting.md`
- Airflow 3 학습 노트: `docs/airflow3-learnings.md`
- 인프라 결합점·발급 절차: `nexus-prime:docs/dev-guide.md`

## 의심어

"n8n" 등장 시 의심. 옛 repo prefix 발견 시 즉시 placeholder.

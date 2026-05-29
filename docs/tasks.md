# Tasks

## 현재 상태

**플랫폼 가동 — 컨트롤 플레인 (ops-vm) + 워커 2 (worker-vm, mac-server), Edge Executor.** 인프라 layer 는 `nexus-prime` 으로 분리 완료.

**lol-list 워크로드 + `deploy.py` 제거 (2026-05-30)** — lol DAG 3종·전용 인프라(bind mount/PYTHONPATH/도메인 deps/변수) 및 git-clone 기반 `deploy.py`·Variables import 메커니즘 일체 제거. 현재 운영 DAG (`cleanup_logs`/`test_environment`) 만 가동.

**방향 확정: 워크로드 task 의 기본 = `@task.docker`** (라이브러리·숨길 로직은 task 이미지로, `decisions.md` L24/L26). airflow 이미지는 thin (edge3 만).

다음 액션: 차기 도메인 워크로드 미정. DAG 파일 운반 방법은 열린 결정 (`decisions.md` R2).

## Phase 0 — 옛 자산 정리

- [x] ~~옛 n8n 워크플로 export 백업~~ — 불필요, 생략
- [x] 옛 OCI 인스턴스·Boot 볼륨·VCN 일체 termination (2026-05-22)
- [x] 옛 OCI API key 회수 (2026-05-22)
- [x] DNS `n8n.<your-domain>` 레코드 제거
- [x] 옛 repo archive 표기

## Phase 1, 2, 5 인프라 — `nexus-prime` 으로 이주 (2026-05-24)

OCI 재구축 (Phase 1) / 호스트 부트스트랩 (Phase 2) / M1 인프라 부분 (Phase 5: Tailscale, Homebrew, Colima, LaunchAgent) = 모두 `nexus-prime`. 작업 자체는 2026-05-23 완료, 코드·문서만 이주. history 는 git log + nexus-prime decisions cross-ref.

## Phase 3 — 컨트롤 플레인 (ops-vm, airflow 부분만)

- [x] `infra/airflow.Dockerfile`: `apache/airflow:3.2.1` + `apache-airflow-providers-edge3` (현재 3.6.0)
- [x] `infra/ops-vm/docker-compose.yml`: api-server + scheduler + dag-processor + edge-worker-ops (postgres·caddy 는 nexus-prime)
- [x] env: EdgeExecutor / DB conn / Fernet / auth manager / Edge API URL / JWT secret
- [x] DB 초기화 (airflow-init)
- [x] admin auto password
- [x] `docker compose up -d` → HTTP 200 (2026-05-23)

## Phase 4 — 안정 워커 (worker-vm, airflow 부분만)

- [x] `infra/worker-vm/docker-compose.yml`: edge worker `--queues default --concurrency 4`
- [x] env: Edge API URL (Tailscale), JWT, Fernet
- [x] edge worker → api-server 연결 확인
- [x] Edge Executor UI 정상 (2026-05-23)
- [x] dummy task 라우팅 확인 (2026-05-23)

## Phase 5 — M1 워커 (mac-server, airflow 부분만)

- [x] `infra/mac-server/docker-compose.yml` (queue `gpu,default --concurrency 8`)
- [x] `.env` (JWT/Fernet 동일, Tailscale 경로)
- [x] `docker compose up -d` (2026-05-23)
- [x] UI Edge Workers 탭 확인 (2026-05-23)
- [x] `queue="gpu"` 라우팅 검증 (2026-05-24)
- [ ] sleep/wake 후 worker 자연 복귀 (Phase 7 과 합쳐도 됨)

## Phase 6 — lol-list 이관 (제거됨 2026-05-30)

~~lol-list ETL 3종 이관·엔드투엔드 검증 완료 (2026-05-23).~~ 워크로드 제거로 무효.

## Phase 7 — 플랫폼 검증

워크로드와 무관한 플랫폼 안정성 (운영 DAG / `test_environment` 로 확인 가능):

- [ ] DAG Versioning UI 확인
- [ ] M1 power-cycle 시 gpu queue 자연 복귀
- [ ] M1 sleep 중 in-flight task zombie 처리 후 retry

## Phase 8 — 모니터링 (Phase 7 후)

nexus-prime 가 `prometheus.internal` 제공 (dev-guide) — airflow StatsD/metrics 노출 후 scrape 대상 등록은 nexus-prime 측. Cloud Guard/Budget 은 OCI 레벨 (nexus-prime).

- [ ] airflow metrics 노출 (StatsD exporter → prometheus.internal scrape)
- [ ] Notification + 알람 3종

## 실행 환경 격리 — `@task.docker` (표준)

운용 (scheduler·worker) ↔ 실행 (task body) 환경 분리 = 모든 워크로드의 기본 (`decisions.md` L24/L26). 첫 도메인 워크로드 도입 시 구체화할 항목:

- [ ] task image Dockerfile (`python:3.12-slim` + 도메인 deps). capability 분기 (`task-default` / `task-gpu`) 필요 시
- [ ] 빌드·push → `registry.internal/<image>:<sha>` (절차·insecure-registries = `nexus-prime:docs/dev-guide.md`)
- [ ] DAG: `@task.docker(image=..., force_pull=False)`, 자격증명 env 주입 (워커 `.env` 경유)
- [ ] DooD: 3 노드 워커 compose 에 `/var/run/docker.sock` bind mount
- [ ] sha-pinned image 캐시 동작 검증 (노드별 첫 pull, 이후 hit)

## 미래

- v2: `@asset` (lineage 가 운영 핵심인 워크로드 등장 시)
- v2: DAG bundle (remote storage)
- v2: Triggerer (deferrable 필요 시)
- v2: GitHub Actions → registry push 자동화

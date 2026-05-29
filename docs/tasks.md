# Tasks

## 현재 상태

**Phase 6 — lol-list 이관 완료 (2026-05-23). 3개 DAG 엔드투엔드 검증 완료.**

진행 중: **인프라 layer 분리** (2026-05-24 ~) — airflow-stack 에서 인프라 부분이 `nexus-prime` repo 로. 현재 repo 는 airflow workload 만. 코드 변경만, 현 환경 무영향.

다음 액션: Phase 9 — 실행 환경 격리 (`@task.docker` + lol-list 패키지화). Phase 7 검증 / Phase 8 모니터링은 Phase 9 후로 미룸.

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

## Phase 6 — lol-list 이관

- [x] DAG 3개: `sync_matches` / `sync_liquipedia` / `sync_lol_meta` (2026-05-23)
- [x] Airflow Variables IaaC (airflow-init 의 import)
- [x] 배포 DAG `dags/deploy.py` (2026-05-23) — Phase 9 에서 폐기 예정
- [x] lol-list deps Dockerfile 추가 — Phase 9 에서 task image 로 이전
- [x] 엔드투엔드 검증 (2026-05-23)
- [ ] `docs/spec.md` 에 Supabase 스키마
- [ ] 스케줄 자동 fire 확인 (10분 / 15분 / 일별 KST 00:00)

## Phase 7 — 검증

- [ ] 첫 자동 run 성공
- [ ] 24시간 안정 동작
- [ ] DAG Versioning UI 확인
- [ ] M1 power-cycle 시 gpu queue 자연 복귀
- [ ] M1 sleep 중 in-flight task zombie 처리 후 retry

## Phase 8 — 모니터링 (Phase 7 후)

nexus-prime 가 `prometheus.internal` 제공 (dev-guide) — airflow StatsD/metrics 노출 후 scrape 대상 등록은 nexus-prime 측. Cloud Guard/Budget 은 OCI 레벨 (nexus-prime).

- [ ] airflow metrics 노출 (StatsD exporter → prometheus.internal scrape)
- [ ] Notification + 알람 3종

## Phase 9 — 실행 환경 격리 (2026-05-24 계획)

L23~L26. 운용 환경 (scheduler·worker) ↔ 실행 환경 (task body) 분리.

해소되는 이슈:
- lol-list transitive deps 가 airflow-stack 이미지에 강제 박힘 → L23·L24
- 노드별 환경 분기 부재 → L24
- lol-list 패키지 아님 → L23
- bind mount + `git pull` = 비-self-contained → L24·L26
- import 캐시 vs 핫스왑 비결정 → L24
- `deploy.py` queue 라우팅 비결정 → L24 폐기

### 인프라 선행 (nexus-prime 에서)

- [ ] worker-vm 재생성 (75 → 50 GB), ops-vm 부트 확장 (125 → 150) — `nexus-prime:tofu` apply
- [ ] `nexus-prime:compose/registry/` 가동 (이미 구성 완료, 실 배포 미진행)

### 코드·이미지

- [ ] lol-list `pyproject.toml` + dep 선언, `pip install` 가능
- [ ] task image Dockerfile (lol-list 안) — `python:3.12-slim` + `pip install lol-list@<sha>`. capability 분기 (`task-default` / `task-gpu`) 필요 시
- [ ] 빌드·push — 수동 또는 GitHub Actions → `registry.internal/lol-list:<sha>` (push 절차·insecure-registries 설정은 `nexus-prime:docs/dev-guide.md`)

### Airflow 전환

- [ ] DAG: `sync_*` → `@task.docker(image=..., force_pull=False)`. SUPABASE 자격 env 주입
- [ ] DooD: 3 노드 워커 compose 에 `/var/run/docker.sock` bind mount
- [ ] `airflow.Dockerfile` 에서 lol-list 도메인 deps 제거
- [ ] 워커 compose 의 lol-list bind mount + `PYTHONPATH` 제거 (3 노드)
- [ ] `dags/deploy.py` 삭제. `dags/cleanup_logs.py` 는 PythonOperator 유지
- [ ] `dags/` 자체는 dag-processor `:ro` bind mount 유지 (image 굽기는 v2)

### 검증

- [ ] `sync_matches` task 가 registry 컨테이너에서 실행 + Supabase upsert 정상
- [ ] sha-pinned image: 노드별 첫 task pull, 이후 캐시 hit
- [ ] worker-vm 재생성 후 동일 sha 동일 동작

### 문서 갱신 (진행 중)

- [ ] `architecture.md` — Phase 9 후 task image 2 층 모델
- [ ] `setup.md` — task image 빌드·push 절차
- [ ] `runbook.md` — image 빌드·push 일상 절차, worker-vm 재생성은 nexus-prime 측

## 미래

- v2: `@asset` (derived data 등장 시)
- v2: DAG bundle (remote storage)
- v2: Triggerer (deferrable 필요 시)
- v2: GitHub Actions → registry push 자동화
- v2: `@task.external_python` (task image 매트릭스 부담 시)

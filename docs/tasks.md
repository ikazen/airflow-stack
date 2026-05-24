# Tasks

## 현재 상태

**Phase 6 — lol-list 이관 완료 (2026-05-23). 3개 DAG 엔드투엔드 검증 완료.**

다음 액션: Phase 9 — 실행 환경 격리 + registry 전환 (L23~L26, 2026-05-24 결정). Phase 7 검증·Phase 8 모니터링은 Phase 9 후로 미룸 (배포·실행 모델이 바뀌므로 검증 기준선이 달라짐).

## Phase 0 — 옛 자산 정리

- [x] ~~옛 n8n 워크플로 export 백업~~ — 불필요, 생략
- [x] 옛 OCI 인스턴스·Boot 볼륨·VCN 일체 termination (2026-05-22). 하위 컴파트먼트 없음 → 컴파트먼트 termination 해당 없음
- [x] 옛 OCI API key 회수 (2026-05-22 — 미사용 2개 삭제, 현용 1개 유지)
- [x] DNS `n8n.<your-domain>` 레코드 제거 — 외부 DNS, 콘솔에서 수동
- [x] 옛 repo archive 표기 — 옛 GitHub repo README, 수동

## Phase 1 — OCI 재구축

- [x] 컴파트먼트 `main` / VCN `main-vcn` / 서브넷 2 / NSG 2 / Gateway 3 / Route table (2026-05-22)
- [x] reserved public IP (ops-vm)
- [x] ops-vm + worker-vm 프로비저닝 (A1.Flex 2/12, boot 125/75 GB)
- [x] ~~Boot volume 백업~~ — 안 함 (전체 disposable, L10)
- [x] DNS `airflow.<your-domain>` A 레코드 — 외부 DNS, 수동

## Phase 2 — 호스트 부트스트랩

- [x] `scripts/host-setup.sh` (swap, Docker + Compose, unattended-upgrades) — 양쪽 VM 완료 (2026-05-22)
- [x] Tailscale ops-vm·worker-vm 가입 (2026-05-23). M1 은 Phase 5 전 수동
- [x] Tailscale ACL — 단일 사용자, 기본값으로 충분
- [x] worker-nsg 임시 SSH 룰 제거. ops-nsg 본인 IP /32 fallback 유지

## Phase 3 — 컨트롤 플레인 (ops-vm)

- [x] `infra/airflow.Dockerfile`: `apache/airflow:3.2.1` + `apache-airflow-providers-edge3>=3.5.0` (현재 3.6.0)
- [x] `infra/ops-vm/docker-compose.yml`: postgres + api-server + scheduler + dag-processor + caddy
- [x] env: EdgeExecutor / DB conn / Fernet / auth manager / Edge API URL / JWT secret (`.env`, repo 외)
- [x] DB 초기화: `airflow db migrate` (airflow-init 서비스)
- [x] admin: `AIRFLOW__SIMPLE_AUTH_MANAGER__USERS=admin`. 비번 자동 생성 → `docker exec api-server cat .../simple_auth_manager_passwords.json.generated`
- [x] `dags/` 폴더 생성
- [x] Caddyfile: `/edge_worker/v1/*` 차단 (Tailscale 직결 전용), 나머지 api-server:8080 프록시
- [x] `docker compose up -d` → `https://airflow.<your-domain>` HTTP 200 확인 (2026-05-23)

## Phase 4 — 안정 워커 (worker-vm)

- [x] git clone (dags/ 코드 운반)
- [x] `infra/worker-vm/docker-compose.yml`: 커스텀 이미지로 `airflow edge worker --queues default --concurrency 4`
- [x] env: Edge API URL (Tailscale 경로), JWT, Fernet (`.env`, repo 외)
- [x] edge worker → api-server 연결 확인 (5초 polling, 200 OK)
- [x] Edge Executor UI (`/plugin/edge_executor`) 정상 (2026-05-23)
- [x] dummy task 라우팅 확인 — test_environment DAG, ops-vm/worker-vm 각각 정상 (2026-05-23)

## Phase 5 — M1 워커 (mac-server)

런타임 결정: **Colima + 컨테이너** (Docker Desktop 라이선스·GUI 부담 회피, 통일성 위해 같은 compose 사용). uv 호스트 직접 안 함.

- [x] Tailscale 가입 + SSH alias `mac` (2026-05-23)
- [x] Homebrew + `brew install colima docker docker-compose` (2026-05-23)
- [x] colima 4 CPU / 8 GB VM 시동 (2026-05-23)
- [x] SSH ed25519 키 생성 + GitHub deploy key (lol-list, read-only) 등록 (2026-05-23)
- [x] `~/airflow-stack`, `~/lol-list` git clone (2026-05-23)
- [x] `infra/mac-server/docker-compose.yml` (queue `gpu,default --concurrency 8`, hostname `mac-server`)
- [x] `.env` 작성 (ops-vm JWT/Fernet 동일값, Tailscale 경로)
- [x] `docker compose up -d` → DB 에 `mac-server` idle 등록 확인 (2026-05-23)
- [x] UI Edge Workers 탭 시각 확인 (2026-05-23)
- [x] `queue="gpu"` dummy task 라우팅 검증 — test_environment 3 task 모두 success, gpu→mac-server 정확 (2026-05-24)
- [x] LaunchAgent: `local.airflow.colima` colima 자동 시동 (2026-05-23). edge worker 는 compose `restart: unless-stopped` 가 처리. 절차 `docs/runbook.md`
- [ ] sleep/wake 후 worker 자연 복귀 확인 (Phase 7 검증과 합쳐도 됨)

## Phase 6 — lol-list 이관

`docs/asset-model.md` 의 v1 결정 (전통 `@dag`) 따름.

아키텍처: thin wrapper DAG (airflow-stack, public) + 비즈니스 로직 (lol-list repo, private). PYTHONPATH로 연결.

- [x] DAG 3개 작성: `dags/sync_matches.py` / `sync_liquipedia.py` / `sync_lol_meta.py` (2026-05-23)
- [x] Airflow Variables IaaC: `airflow-variables.json` → airflow-init 에서 import (2026-05-23)
- [x] 배포 DAG: `dags/deploy.py` — repo·commit 파라미터로 양쪽 VM git pull (2026-05-23)
- [x] lol-list 의존성 Dockerfile 추가: postgrest, httpx, bs4, lxml, tenacity (2026-05-23)
- [x] 엔드투엔드 검증: leagues 4, teams 42, matches 160, liquipedia 14 upserted (2026-05-23)
- [ ] `docs/spec.md` 에 Supabase 스키마 기록
- [ ] 스케줄 자동 fire 확인 (10분 / 15분 / 일별 KST 00:00)

## Phase 7 — 검증

- [ ] 첫 자동 run 성공 (UI DagRun)
- [ ] 24시간 안정 동작
- [ ] DAG Versioning UI 확인 (코드 변경 → 새 version)
- [ ] M1 power-cycle 시 gpu queue 자연 복귀
- [ ] M1 sleep 중 in-flight task 가 zombie 처리 후 retry 되는지 확인
- [ ] 옛 repo README archive 표기

## Phase 8 — 모니터링 (Phase 7 검증 후)

- [ ] Cloud Guard ON
- [ ] Notification + 알람 3종 (CPU>80% 10분 / 인스턴스 not RUNNING / Boot vol >85%)
- [ ] Budget alert

## Phase 9 — 실행 환경 격리 + registry (2026-05-24 계획)

L23~L26 결정. 운용 환경 (scheduler·worker) ↔ 실행 환경 (task body) 분리, lol-list 패키지화, 자가호스트 registry, sha-pinned image.

해소되는 이슈 (2026-05-24 회고):
- lol-list transitive deps 가 airflow-stack 이미지에 강제 박힘 → L23·L24
- 노드별 환경 분기 (gpu/cpu/browser) 표현 수단 부재 → L24 (task image 단위 분기)
- lol-list = 패키지 아닌 디렉토리 (`from collectors import X` 작동 위해 PYTHONPATH 트릭) → L23
- bind mount `:ro` + 호스트 `git pull` = 컨테이너 비-self-contained, 버전 추적 불가 → L24·L26
- worker 프로세스 import 캐시 vs `git pull` 핫스왑 = 새 코드/옛 코드 섞임 비결정 → L24
- `dags/deploy.py` 의 queue=default 라우팅 비결정 (worker-vm 가 갱신 안 될 수 있음) → L24 가 deploy DAG 자체 폐기 (배포 = registry push 로 이동)

### 인프라 (선행)

- [ ] worker-vm 재생성: 부트 75 → 50 GB, Tailscale 재가입, host-setup 재실행, edge worker compose 재기동
- [ ] OCI Block Volume 25 GB 생성 (ops-vm 과 같은 AD), ops-vm attach (paravirtualized), `mkfs.ext4` → `/srv/registry` mount, `/etc/fstab` 등록
- [ ] `infra/ops-vm/docker-compose.yml` 에 `registry:2` 서비스 추가 — `/srv/registry` bind, tailnet IP bind 또는 Caddy 뒤 reverse proxy
- [ ] registry retention 정책: `keep last 10 tags` + `untagged > 30d` GC cron

### 코드·이미지 (병행)

- [ ] lol-list repo 에 `pyproject.toml` + dep 선언 (httpx / bs4 / lxml / postgrest / tenacity / ...), `pip install` 가능 확인
- [ ] `infra/task/Dockerfile` (또는 lol-list 안) — 베이스 `python:3.12-slim` + `pip install lol-list@<sha>`. 노드 capability 별 변종 (`task-default`, `task-gpu`) 필요 시
- [ ] 빌드·push 흐름: 수동 `make` 또는 GitHub Actions → `registry.<tailnet>:5000/lol-list:<sha>` push (선택: GitHub Actions 자동화는 후순위)

### Airflow 전환

- [ ] DAG 전환: `sync_matches` / `sync_liquipedia` / `sync_lol_meta` → `@task.docker(image=..., force_pull=False)` 로 thin wrapper. SUPABASE 자격은 `env_file` 또는 `mounts` 로 주입
- [ ] DooD: `infra/ops-vm/docker-compose.yml` · `infra/worker-vm/docker-compose.yml` · `infra/mac-server/docker-compose.yml` 워커 서비스에 `/var/run/docker.sock` bind mount 추가
- [ ] `airflow.Dockerfile` 에서 lol-list 도메인 deps (httpx / bs4 / lxml / postgrest / tenacity) 제거 — task image 가 책임짐
- [ ] 워커 compose 의 lol-list bind mount 제거 (3 노드), `PYTHONPATH` 제거
- [ ] `dags/deploy.py` 삭제 (queue 비결정 + 비-immutable 폐기). `dags/cleanup_logs.py` 는 운용 작업이라 PythonOperator 유지
- [ ] `dags/` 자체는 여전히 dag-processor 가 import — 현재 `:ro` bind mount 유지 (image 굽기는 v2)

### 검증

- [ ] `sync_matches` task 가 `registry.<tailnet>:5000/lol-list:<sha>` 컨테이너에서 실행되어 Supabase upsert 정상
- [ ] sha-pinned image: 노드별 첫 task pull, 이후 task 캐시 hit (`docker images` 확인)
- [ ] worker-vm 재생성 후 동일 sha 로 동일 동작
- [ ] registry 디스크 모니터 — `/srv/registry` 사용량, GC 동작

### 문서 갱신 (작업 진행 중)

- [ ] `architecture.md` — queue 의미 명시, OCI 자원에 block volume / registry 노드, 이미지 2 층 모델 (baseline + task image)
- [ ] `setup.md` — Phase 4 (worker-vm) 재생성 절차, Phase 9 (registry + task image 빌드·push)
- [ ] `runbook.md` — image 빌드·push 일상 절차, registry GC, worker-vm 재생성 절차 박제

## 미래

- v2: derived data 추가 시 `@asset` 도입 (`docs/asset-model.md` 트리거)
- v2: DAG bundle (remote storage) 검토 — task image 와 별개로 DAG 파일 운반 자동화
- v2: Triggerer (deferrable 필요 시)
- v2: GitHub Actions → registry push 자동화 (현재 수동 빌드)
- v2: `@task.external_python` (venv 매트릭스) — task image 매트릭스가 부담스러워지면 같은 노드 안에서 가벼운 분기 수단

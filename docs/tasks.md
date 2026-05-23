# Tasks

## 현재 상태

**Phase 6 — lol-list 이관 완료 (2026-05-23). 3개 DAG 엔드투엔드 검증 완료.**

다음 액션: Phase 7 검증 (24h 안정 동작) → Phase 8 모니터링. Phase 5 (M1 워커) 는 맥북 준비 시.

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
- [ ] `queue="gpu"` dummy task 라우팅 검증 (test_environment DAG)
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

## 미래

- v2: derived data 추가 시 `@asset` 도입 (`docs/asset-model.md` 트리거)
- v2: DAG bundle (remote storage) 검토
- v2: Triggerer (deferrable 필요 시)

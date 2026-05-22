# Tasks

## 현재 상태

**Phase 0 시작 전 — 문서화 진행 중.**

다음 액션: 열린 결정 O1~O4 답 확정 → Phase 0.

## Phase 0 — 옛 자산 정리

- [ ] 옛 `<previous-stack>` 워크플로 export 마지막 백업
- [ ] 옛 OCI Instance → Boot Volume → VNIC → Subnet → VCN → 컴파트먼트 termination
- [ ] DNS 의 `<previous-subdomain>.<your-domain>` 제거
- [ ] 옛 OCI API key 회수
- [ ] 옛 repo archive 메모

## Phase 1 — OCI 재구축

- [ ] 새 컴파트먼트 / VCN / 서브넷 / NSG / Gateway / Route table
- [ ] reserved public IP (ops-vm)
- [ ] ops-vm (2 OCPU/12GB) + worker-vm (2 OCPU/12GB) 프로비저닝
- [ ] Boot volume Bronze 백업
- [ ] DNS `airflow.<your-domain>` A 레코드
- [ ] Cloud Guard / Notification / 알람 3종 / Budget (O4)

## Phase 2 — 호스트 부트스트랩

- [ ] `scripts/host-setup.sh` (swap, Docker + Compose, fail2ban, unattended-upgrades)
- [ ] Tailscale 3 노드 가입
- [ ] Tailscale ACL — SSH tailnet 만, ops-vm 443/80 공인
- [ ] OCI NSG SSH 룰 본인 IP /32 fallback 만

## Phase 3 — 컨트롤 플레인 (ops-vm)

- [ ] `infra/airflow.Dockerfile`: `apache/airflow:3.2.x` + `apache-airflow-providers-edge3` + 도메인 deps (constraints 핀)
- [ ] `infra/ops-vm/docker-compose.yml`: postgres + (커스텀 이미지) api-server + scheduler + dag-processor + caddy
- [ ] env: EdgeExecutor / DB conn / Fernet / auth manager / Edge API URL / JWT secret
- [ ] DB 초기화: `airflow db migrate` (init 서비스 또는 1회 실행)
- [ ] admin: `simple_auth_manager_users` 설정. 비번은 Airflow 자동 생성 → `simple_auth_manager_passwords.json.generated` 확인
- [ ] `dags/` 폴더 (dag-processor 가 읽음)
- [ ] Caddyfile (TLS 만. 사람용 UI 만 공개, `/edge_worker/*` 비공개 — L20)
- [ ] `.env` 시크릿 (repo 외 보관)
- [ ] `docker compose up -d` → `https://airflow.<your-domain>` 접속 확인

## Phase 4 — 안정 워커 (worker-vm)

- [ ] git clone (dags/ + src/ 코드 운반)
- [ ] `infra/worker-vm/docker-compose.yml`: 커스텀 이미지로 `airflow edge worker --queues default --concurrency 4`
- [ ] repo 마운트: `dags/` → DAG, `src/` → `PYTHONPATH` (워커가 `collectors` import)
- [ ] env: Edge API URL (Tailscale 경로), JWT, Fernet
- [ ] UI Edge Workers 탭 healthy 확인
- [ ] dummy task 라우팅 확인

## Phase 5 — M1 워커 (macbook)

- [ ] uv + Python 3.12, git clone, `uv sync --frozen` (edge3 포함)
- [ ] 컨테이너 vs 호스트 직접 결정 (Docker Desktop 부담 평가)
- [ ] `~/Library/LaunchAgents/<reverse-domain>.airflow-worker.plist`: `--queues mac --concurrency 2`, KeepAlive, RunAtLoad
- [ ] `launchctl load`
- [ ] UI `mac` queue healthy, sleep/wake 자동 복귀 확인

## Phase 6 — lol-list 이관

`docs/asset-model.md` 의 v1 결정 (전통 `@dag`) 따름.

- [ ] `src/collectors/` 에 옛 도메인 코드 이관
- [ ] `dags/lol_list_matches.py` / `lol_list_liquipedia.py` / `lol_list_meta.py`
- [ ] `docs/spec.md` 에 Supabase 스키마 기록
- [ ] 스케줄 자동 fire 확인 (10분 / 15분 / 일별 KST 00:00)

## Phase 7 — 검증

- [ ] 첫 자동 run 성공 (UI DagRun)
- [ ] 24시간 안정 동작
- [ ] DAG Versioning UI 확인 (코드 변경 → 새 version)
- [ ] M1 power-cycle 시 mac queue 자연 복귀
- [ ] M1 sleep 중 in-flight task 가 zombie 처리 후 retry 되는지 확인
- [ ] 옛 repo README archive 표기

## 미래

- v2: derived data 추가 시 `@asset` 도입 (`docs/asset-model.md` 트리거)
- v2: DAG bundle (remote storage) 검토
- v2: Triggerer (deferrable 필요 시)

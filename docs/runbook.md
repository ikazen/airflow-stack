# Runbook

Airflow workload 운영 절차. 인프라 운영 (호스트 재생성·볼륨·DNS·Caddy·Tailscale·Postgres DB 추가·Registry GC) 은 `nexus-prime:docs/runbook.md`.

## mac-server colima 자동 시동

LaunchAgent 정의·설치 절차 → **`nexus-prime:hosts/mac-server/launchd/local.airflow.colima.plist`** + `nexus-prime:hosts/mac-server/README.md`.

colima 가 떠 있으면 edge worker 컨테이너는 `restart: unless-stopped` 가 자체 복귀.

## Airflow DB / user 초기화 (최초 1회)

ops-vm 에서 실행. postgres admin 패스워드는 Bitwarden `secrets-backup.md` 참조.

```bash
ssh ops-vm

docker exec -it postgres psql -U postgres -c "CREATE DATABASE airflow;"
docker exec -it postgres psql -U postgres -c "CREATE USER airflow WITH PASSWORD '<airflow-db-pw>';"
docker exec -it postgres psql -U postgres -c "GRANT ALL ON DATABASE airflow TO airflow;"
```

완료 후 `airflow-init` 이 `db migrate` 로 스키마 생성.

PostgreSQL 15+ 는 public 스키마 CREATE 권한이 기본 미포함 — 아래도 실행:
```bash
docker exec -it postgres psql -U postgres -d airflow -c "GRANT ALL ON SCHEMA public TO airflow;"
```

Airflow Variables 로 task 자격증명 주입 (Connections 미사용). `airflow-variables.json` 참조 (gitignore 됨).

현재 사용 중인 Variables:

| key | 용도 |
|---|---|
| `db_url` | lck-pics Supabase URL |
| `db_key` | lck-pics Supabase service role key |
| `rondo_db_url` | reflexion-rondo DB URL |
| `ollama_base_url` | Ollama API 엔드포인트 |
| `ollama_cloud_base_url` | Ollama 클라우드 API 엔드포인트 |
| `ollama_api_key` | Ollama API key |
| `minio_endpoint` | MinIO S3 엔드포인트 |
| `minio_access_key_id` | MinIO access key |
| `minio_secret_access_key` | MinIO secret key |
| `rondo_model_coder` | rondo coder LLM 모델명 |
| `rondo_model_strategist` | rondo strategist LLM 모델명 |
| `rondo_model_reflector` | rondo reflector LLM 모델명 |
| `rondo_model_embedding` | rondo embedding 모델명 |
| `rondo_task_image_version` | reflexion-rondo task 이미지 태그(예: `v1.2.27`). `reflexion_rondo_deploy` DAG가 빌드 후 bump — git push 아님, 즉시 반영(decisions.md L29) |

Variables 는 UI (Admin → Variables) 또는 `airflow-variables.json` import 로 복구. 메타 DB 손실 시 `airflow-variables.json` (로컬 백업) 에서 재import.

**최초 설정 필요**: `rondo_task_image_version`은 `reflexion_rondo_deploy` DAG를 처음 도입할 때 현재 라이브 태그로 1회 수동 설정해야 한다(UI Admin → Variables, 또는 `airflow variables set rondo_task_image_version v1.2.26`) — 비어있으면 `reflexion_rondo_cycle`의 이미지 참조가 깨진다.

## 코드 배포

| 무엇을 바꿨나 | 배포 |
|---|---|
| **DAG 파일** (`dags/`) | repo 에 `git push` → GitDagBundle 이 `refresh_interval` (60s) 마다 자동 fetch. 호스트 작업 0 |
| **task body 로직·라이브러리** | `@task.docker` 이미지 빌드 → `registry.internal` push (절차·insecure-registries = `nexus-prime:docs/dev-guide.md`). DAG 의 image 태그를 새 sha 로 갱신 후 push |
| **reflexion-rondo daemon+task 이미지** | Airflow UI에서 `reflexion_rondo_deploy` DAG를 `{"tag": "vX.Y.Z"}` conf로 트리거(Trigger DAG w/ config) — `ops` 큐가 clone+build+push까지 수행(decisions.md L29), task는 Variable bump로 즉시 반영. daemon의 실제 배포(compose.yml+재시작)는 reflexion-rondo `deploy/release.sh vX.Y.Z`로 별도 실행 |
| **인프라** (compose / Dockerfile / `.env`) | 해당 호스트 `cd ~/projects/airflow-stack && git pull` + `docker compose -f infra/<host>/docker-compose.yml up -d --build`. Dockerfile (provider 핀 등) 변경은 `--build` 필수 — `infra/airflow.Dockerfile`에 `git` CLI 추가(issue #2)로 **3개 호스트(ops-vm/worker-vm/mac-server) 전부 재빌드 필요** |

호스트 배포 경로 = `~/projects/airflow-stack` (mac 은 `/Users/.../projects/...`). SSH = tailnet alias `ops-vm`/`worker-vm`/`mac-server`.

## 워커 재기동 / recreate

config 만 바뀌어 워커를 recreate 하면 (`.env` 변경 후 `up -d`), **빠른 recreate 시 이름 충돌**이 난다: 옛 등록이 api-server 에 아직 active → `A worker with the name '<host>' is already active` crash-loop. 해소:

```bash
# 각 호스트: crash-loop 정지
docker compose -f infra/<host>/docker-compose.yml stop <edge-worker-service>
# ops-vm dag-processor 컨테이너에서: stale 등록 삭제 (건드린 워커만)
airflow edge remove-remote-edge-worker -H <hostname>
# 각 호스트: 재시작
docker compose -f infra/<host>/docker-compose.yml start <edge-worker-service>
```

정상 신호 = 로그 `No new job to process`. 확인 = `airflow edge list-workers` (전 워커 idle). 안 건드린 워커의 등록은 삭제 금지 (그 워커도 충돌남).

mac-server compose 에 drain 설정이 박혀 있어 (`DRAIN_TIMEOUT_SEC=10`, `DRAIN_KILL_GRACE_SEC=5`, `stop_grace_period: 30s`) force-recreate 시 ~15s 안에 OFFLINE 등록 완료 — 이 값은 수정하지 말 것 (`airflow3-learnings.md` sleep/wake 섹션 참조).

## 메타 DB 정리 (db clean) — host-level, 수동

`airflow db clean` 은 **DAG task 로 불가** — Airflow 3 Task SDK 가 task 에 DB 접속(`SQL_ALCHEMY_CONN`)을 주지 않음 (`Could not parse SQLAlchemy URL` 로 실패). task 는 api-server 경유만, DB 직접 접근은 컨트롤 플레인 컨테이너에서.

→ 필요 시 ops-vm 에서 직접 (scheduler 컨테이너 = DB 권한 보유, task sandbox 아님):

```bash
ssh ops-vm
docker compose -f ~/projects/airflow-stack/infra/ops-vm/docker-compose.yml exec -T scheduler \
  airflow db clean --clean-before-timestamp "$(date -u -d '14 days ago' --iso-8601=seconds)" --skip-archive -y
```

현재 **수동/필요 시**. 메타 DB 가 disposable·소형(~11MB, 워크로드 없어 거의 안 늘어남)이라 상시 불필요(YAGNI). 고volume 워크로드 도입으로 실제 누적되면 systemd timer + 모니터링(systemd `OnFailure` 알림, 또는 node_exporter textfile → `prometheus.internal` staleness alert)으로 자동화 — 그때 결정.

## 상태 점검 (Claude / 자동화용)

점검 도구는 Airflow REST API (api-server) + rondo DB 직접 조회 2가지. 설정값은 로컬 `~/.config/claude/ops-status.env` (600, repo 밖) 에만 — 실제 값·host는 여기 기록 안 함.

### Airflow API (인증 불필요/필요 혼재)

| 항목 | 엔드포인트 |
|---|---|
| 헬스 (인증 불필요) | `GET /api/v2/monitor/health` |
| DAG Run 목록 | `GET /api/v2/dags/~/dagRuns?order_by=-start_date&limit=20` |
| 실패 task | `GET /api/v2/dags/~/dagRuns/~/taskInstances?state=failed` |
| DAG 파싱 오류 | `GET /api/v2/importErrors` |
| Task 로그 | `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}` |

토큰: `POST /auth/token` `{"username": "...", "password": "..."}` → `access_token` (HMAC, 매 호출 시 재발급).

### rondo DB (read-only)

API 없음 → psql/psycopg 직접. role `ro_claude` (`pg_read_all_data` 부여, ops-vm Postgres).
접속: ops-status.env 의 `RONDO_DB_URL`.
주요 테이블: `raw.attempts`, `raw.competitions`, `raw.cycle_queue`, `raw.kaggle_submissions`.

## 작성 예정

- Secrets 회전 (Fernet / JWT — Postgres pw 는 nexus-prime)
- 인스턴스 / 메타 손실 시 재배포 복구 (백업 없음 — nexus-prime L7)

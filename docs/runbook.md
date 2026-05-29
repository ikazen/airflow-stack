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

Airflow Variables / Connections 는 미사용 — task 자격증명은 워커 `.env` → `@task.docker` 컨테이너 env 주입. 메타 DB 에 코드 아닌 상태 없음 (disposable, `decisions.md` L10).

## 코드 배포

| 무엇을 바꿨나 | 배포 |
|---|---|
| **DAG 파일** (`dags/`) | repo 에 `git push` → GitDagBundle 이 `refresh_interval` (60s) 마다 자동 fetch. 호스트 작업 0 |
| **task body 로직·라이브러리** | `@task.docker` 이미지 빌드 → `registry.internal` push (절차·insecure-registries = `nexus-prime:docs/dev-guide.md`). DAG 의 image 태그를 새 sha 로 갱신 후 push |
| **인프라** (compose / Dockerfile / `.env`) | 해당 호스트 `cd ~/projects/airflow-stack && git pull` + `docker compose -f infra/<host>/docker-compose.yml up -d --build`. Dockerfile (provider 핀 등) 변경은 `--build` 필수 |

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

## 작성 예정

- 일상 헬스 체크 (api-server / scheduler / dag-processor / Edge Workers)
- Secrets 회전 (Fernet / JWT — Postgres pw 는 nexus-prime)
- 인스턴스 / 메타 손실 시 재배포 복구 (백업 없음 — nexus-prime L7)

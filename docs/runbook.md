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

## airflow-variables.json 생성 (최초 1회 / 재배포 시)

gitignored. ops-vm 에서 직접 생성. 형식:

```json
{
  "key1": "value1",
  "key2": "value2"
}
```

실제 값은 Bitwarden `secrets-backup.md` 참조. 파일이 없으면 `airflow-init` 이 `db migrate` 만 실행하고 import 는 건너뜀 (정상 종료).

## 작성 예정

- 일상 헬스 체크 (api-server / scheduler / dag-processor / Edge Workers)
- 코드 배포 (Phase 9 후) — DAG 파일은 git pull, task body 는 `registry.internal` push (push·insecure-registries 절차 = `nexus-prime:docs/dev-guide.md`)
- 워커 재기동 (worker-vm container restart / mac-server `launchctl unload+load` 또는 `docker compose restart`)
- Secrets 회전 (Fernet / JWT — Postgres pw 는 nexus-prime)
- 인스턴스 / 메타 손실 시 재배포 복구 (백업 없음 — nexus-prime L7)

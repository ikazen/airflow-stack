# Setup

Airflow workload 셋업. **사전 조건: `nexus-prime` 셋업 완료** (호스트·Tailscale·Caddy·Postgres·Registry 가동 중). 인프라 셋업은 `nexus-prime:docs/setup.md`, 내부 주소·결합점·Postgres DB 발급·registry push 절차는 `nexus-prime:docs/dev-guide.md`.

## 1. 코드 운반

각 호스트 (compose / Dockerfile / `.env` 용 — 빌드 context):

```
ssh <host>
mkdir -p ~/projects && git clone https://github.com/<your-repo>/airflow-stack.git ~/projects/airflow-stack
```

**DAG 파일은 호스트 체크아웃과 무관** — Airflow 가 GitDagBundle 로 repo 에서 직접 fetch (L27). task body 도메인 코드·라이브러리는 `@task.docker` image (`registry.internal`, L24). 호스트 clone 은 인프라(compose/Dockerfile/.env) 운반·이미지 빌드용일 뿐.

## 2. ops-vm — control plane

```
ssh ops-vm
cd ~/projects/airflow-stack
cp infra/ops-vm/.env.example infra/ops-vm/.env
$EDITOR infra/ops-vm/.env   # Fernet / JWT / Postgres conn / Edge API URL / OPS_TAILNET_IP / DAG bundle repo_url

docker compose -f infra/ops-vm/docker-compose.yml up -d
```

서비스: `airflow-init` (1 회, `airflow db migrate`) → `api-server` / `scheduler` / `dag-processor` / `edge-worker-ops`.

검증:
- `https://airflow.<your-domain>` 로그인 (admin 비번 = ops-vm `~/airflow-local/passwords.json` 의 `admin` 값)
- UI Edge Workers 탭 — `ops-vm` healthy

## 3. worker-vm — stable worker

```
ssh worker-vm
cd ~/projects/airflow-stack
cp infra/worker-vm/.env.example infra/worker-vm/.env
$EDITOR infra/worker-vm/.env   # JWT, Fernet, Edge API URL (Tailscale 경로), DAG bundle repo_url

docker compose -f infra/worker-vm/docker-compose.yml up -d
```

검증: UI Edge Workers 탭 — `worker-vm` healthy.

## 4. mac-server — M1 worker

```
ssh mac-server
cd ~/projects/airflow-stack
cp infra/mac-server/.env.example infra/mac-server/.env
$EDITOR infra/mac-server/.env   # JWT, Fernet, Edge API URL, DAG bundle repo_url

docker compose -f infra/mac-server/docker-compose.yml up -d
```

검증: UI Edge Workers 탭 — `mac-server` healthy, `gpu` queue dummy task 라우팅 정상.

### sleep/wake 자동 복구 (mac 1회 셋업)

mac sleep 중이던 task 가 깨어나면 좀비가 되고 worker 가 wedge 됨 (배경·진단은 `troubleshooting.md`). wake 시 worker 를 깨끗이 재생성해 방지:

`~/.wakeup` (로컬 파일 — git 미포함):
```sh
#!/bin/sh
export DOCKER_HOST="unix:///Users/<your-user>/.colima/default/docker.sock"
COMPOSE=/Users/<your-user>/projects/airflow-stack/infra/mac-server
for i in $(seq 1 30); do /opt/homebrew/bin/docker info >/dev/null 2>&1 && break; sleep 2; done
/opt/homebrew/bin/docker compose -f "$COMPOSE/docker-compose.yml" --env-file "$COMPOSE/.env" up -d --force-recreate edge-worker
```
```
chmod +x ~/.wakeup
brew install sleepwatcher && brew services start sleepwatcher   # -w ~/.wakeup
```
복구 로그: `~/Library/Logs/edge-worker-wake.log`.

손실이 치명적인 default 큐 DAG 에는 `retries` 지정 (예: `daily_meta`). 잦은 DAG (`sync_*`) 는 다음 발화가 흡수하므로 불필요.

## Secrets

- `.env` 커밋 금지 (`.env.example` 만)
- Fernet key 분실 시 암호화 값 복호화 불가 (Connection / Variable 미사용이면 영향 적음)
- 로컬 password manager / secrets vault 보관

## 의존성 핀

`requirements.txt` 생성 시 Airflow constraints 반영:

```
uv pip compile requirements.in -o requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.13.txt
```

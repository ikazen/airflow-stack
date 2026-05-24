# Setup

Airflow workload 셋업. **사전 조건: `nexus-prime` 셋업 완료** (호스트·Tailscale·Caddy·Postgres·Registry 가동 중). 인프라 셋업은 `nexus-prime:docs/setup.md`.

## 1. 코드 운반

각 호스트:

```
ssh <host>
git clone git@github.com:<your-repo>/airflow-stack.git ~/airflow-stack
git clone git@github.com:<your-repo>/lol-list.git ~/lol-list   # SSH deploy key 필요
```

(Phase 9 의 lol-list 패키지화 후엔 task image 가 lol-list 직접 import — 호스트 clone 불필요. 현재 v1 은 PYTHONPATH bind mount 라 호스트에 clone 필요.)

## 2. ops-vm — control plane

```
ssh ops-vm
cd ~/airflow-stack
cp infra/ops-vm/.env.example infra/ops-vm/.env
$EDITOR infra/ops-vm/.env   # Fernet / JWT / Postgres conn / Edge API URL / OPS_TAILNET_IP
cp infra/ops-vm/airflow-variables.json.example infra/ops-vm/airflow-variables.json
$EDITOR infra/ops-vm/airflow-variables.json   # supabase_url / supabase_service_key

docker compose -f infra/ops-vm/docker-compose.yml up -d
```

서비스: `airflow-init` (1 회, db migrate + variables import) → `api-server` / `scheduler` / `dag-processor` / `edge-worker-ops`.

검증:
- `https://airflow.<your-domain>` 로그인 (admin 비번 = `docker exec api-server cat .../simple_auth_manager_passwords.json.generated`)
- UI Edge Workers 탭 — `ops-vm` healthy

## 3. worker-vm — stable worker

```
ssh worker-vm
cd ~/airflow-stack
cp infra/worker-vm/.env.example infra/worker-vm/.env
$EDITOR infra/worker-vm/.env   # JWT, Fernet, Edge API URL (Tailscale 경로)

docker compose -f infra/worker-vm/docker-compose.yml up -d
```

검증: UI Edge Workers 탭 — `worker-vm` healthy.

## 4. mac-server — M1 worker

```
ssh mac-server
cd ~/airflow-stack
cp infra/mac-server/.env.example infra/mac-server/.env
$EDITOR infra/mac-server/.env   # JWT, Fernet, LOL_LIST_PATH, Edge API URL

docker compose -f infra/mac-server/docker-compose.yml up -d
```

검증: UI Edge Workers 탭 — `mac-server` healthy, `gpu` queue dummy task 라우팅 정상.

## 5. lol-list 변수 (Phase 6)

`infra/ops-vm/airflow-variables.json` 의 `supabase_url`, `supabase_service_key` — airflow-init 의 `variables import` 가 자동 적용.

## Secrets

- `.env` 커밋 금지 (`.env.example` 만)
- Fernet key 분실 시 암호화 값 복호화 불가 (Connection / Variable 미사용이면 영향 적음)
- 로컬 password manager / secrets vault 보관

## 의존성 핀

`requirements.txt` 생성 시 Airflow constraints 반영:

```
uv pip compile requirements.in -o requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt
```

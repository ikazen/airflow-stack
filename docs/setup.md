# Setup

인스턴스 / 워커 셋업. Phase 1~5 의 실행 절차.

전제: 열린 결정 확정 — O1·O2·O4 완료. O3 (M1 Tailscale 이름) 은 Phase 5 전.

## OCI 인프라 (Phase 1)

### VCN / 서브넷

```
VCN          <vcn-name>
CIDR         10.0.0.0/16
public       10.0.0.0/24  (ops-vm)
private      10.0.1.0/24  (worker-vm)
Gateway      Internet GW + NAT GW + Service GW
```

### NSG

| NSG | 적용 | Ingress |
|---|---|---|
| `ops-nsg` | ops-vm | 443 + 80 from `0.0.0.0/0`, 22 from 본인 IP /32 (fallback) |
| `worker-nsg` | worker-vm | 없음 (outbound 만) |

노드 간 통신은 Tailscale 위로. subnet 간 ingress 룰 불필요. api-server 8080 은 공인 노출 안 함 (Caddy + Tailscale 워커 경로만 — 아래).

### 인스턴스

| | ops-vm | worker-vm |
|---|---|---|
| Shape | A1.Flex 2/12 GB | A1.Flex 2/12 GB |
| OS | Ubuntu 22.04 ARM | Ubuntu 22.04 ARM |
| Boot | 100 GB Bronze | 50 GB Bronze |
| Public IP | reserved | 없음 |

### Tenancy

API key 정리. Cloud Guard / Notification / 알람 3종 / Budget 는 Phase 8 (검증 후) — `tasks.md`.

### DNS

`airflow.<your-domain>` A → ops-vm reserved IP. 옛 서브도메인 제거.

## 호스트 부트스트랩 (Phase 2)

`scripts/host-setup.sh`: swap, Docker Engine + Compose plugin, fail2ban, unattended-upgrades.

Tailscale 3 노드 가입. ACL: SSH tailnet 만, ops-vm 443/80 공인. MagicDNS ON.

## 컨트롤 플레인 (Phase 3, ops-vm)

### 커스텀 이미지 (L19)

공식 `apache/airflow:3.2.1` 에는 edge3 provider 도 도메인 deps 도 없음 → `infra/airflow.Dockerfile` 로 확장:

```
FROM apache/airflow:3.2.1
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

`requirements.txt` = `apache-airflow-providers-edge3` + `apache-airflow-providers-postgres` + 도메인 deps, constraints 핀 (아래 "의존성 핀"). 공식 이미지는 멀티아치 (arm64 포함) — ARM 그대로 빌드.

api-server·scheduler·dag-processor·워커 전부 이 이미지 사용.

### Compose 서비스

- `postgres:16-alpine` — DB `airflow`, Tailscale interface 또는 localhost 만 bind
- 커스텀 이미지로 `api-server` / `scheduler` / `dag-processor`
- `caddy:2-alpine`

### 환경변수 핵심

```
AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.EdgeExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
AIRFLOW__CORE__FERNET_KEY=<repo 외 보관>
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
  (Airflow 3 기본값 — 명시 핀)
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
AIRFLOW__EDGE__JWT_SECRET=<repo 외 보관>
```

### DB 초기화 + admin

- 최초 1회 `airflow db migrate` (compose 의 init 서비스 또는 단발 실행). Airflow 3 는 `db init` 대신 `db migrate`
- admin user 는 `SIMPLE_AUTH_MANAGER_USERS` (`user:role`) 로 정의. 비번은 사용자가 못 고름 — Airflow 가 자동 생성해 `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` 에 씀. 거기서 읽어 로그인
- SimpleAuthManager 는 2FA·RBAC 없음. 노출 표면 늘릴 거면 `decisions.md` R3 (FabAuthManager) 재고

### Caddyfile

```
airflow.<your-domain> {
    @edge path /edge_worker/*
    respond @edge 404
    reverse_proxy api-server:8080
}
```

공개 endpoint 는 사람용 UI 만. 워커는 Caddy 를 안 거침 (아래).

### 워커 경로 (L20)

api-server 8080 을 ops-vm 의 Tailscale interface 에 bind. 워커는 `http://<ops-vm-tailnet>:8080/edge_worker/v1` 로 직결. Caddy 는 같은 compose 네트워크로 `api-server:8080` 접근 — 공인 포트 불필요.

### 시작

```
docker compose up -d
# https://airflow.<your-domain> → admin 로그인
# 비번 = simple_auth_manager_passwords.json.generated
```

## 안정 워커 (Phase 4, worker-vm)

코드만 git clone, 런타임은 커스텀 이미지:

```
git clone <new-repo> /opt/airflow-stack
```

`infra/worker-vm/docker-compose.yml`: 커스텀 이미지로 `airflow edge worker --queues default --concurrency 4`.

- volume: `/opt/airflow-stack/dags:/opt/airflow/dags:ro`, `/opt/airflow-stack/src:/opt/airflow/src:ro`
- env: `PYTHONPATH=/opt/airflow/src` (워커가 `collectors` import), `AIRFLOW__EDGE__API_URL=http://<ops-vm-tailnet>:8080/edge_worker/v1`, JWT, Fernet

코드 갱신 = `git pull` + `docker compose restart`. deps 갱신 = 이미지 재빌드.

검증: UI Edge Workers 탭 healthy.

## M1 워커 (Phase 5)

```
brew install uv
git clone <new-repo> ~/Code/airflow-stack
cd ~/Code/airflow-stack && uv sync --frozen
```

`uv sync` 가 `apache-airflow` + `apache-airflow-providers-edge3` + 도메인 deps 설치. 컨테이너 vs 호스트 직접은 Docker Desktop on M1 부담 평가 후 결정.

### LaunchAgent

`~/Library/LaunchAgents/<reverse-domain>.airflow-worker.plist`:
- Label: `<reverse-domain>.airflow-worker` (본인 도메인 reverse-DNS)
- WorkingDirectory: `~/Code/airflow-stack`
- EnvVars: `AIRFLOW__EDGE__API_URL=http://<ops-vm-tailnet>:8080/edge_worker/v1`, JWT, Fernet
- ProgramArguments: `[<uv 절대경로>, run, airflow, edge, worker, --queues, mac, --concurrency, 2]`
  (launchd 는 PATH 없음 — `uv` 절대경로 필수. 예: `/opt/homebrew/bin/uv`)
- KeepAlive / RunAtLoad: true
- Logs: `~/Library/Logs/airflow-worker-{out,err}.log`

```
launchctl load ~/Library/LaunchAgents/<reverse-domain>.airflow-worker.plist
```

검증: UI mac queue healthy. sleep/wake 자동 복귀.

## Secrets

- `.env` 커밋 금지. `.env.example` 만 (placeholder 값)
- 로컬 password manager / secrets vault 보관
- Fernet key 분실 시 암호화 값 복호화 불가 → 안전 보관 (단 Connection/Variable 미사용이면 영향 적음)

## 의존성 핀

`pyproject.toml`: `apache-airflow==3.2.x`, `apache-airflow-providers-edge3==<핀>`, `apache-airflow-providers-postgres==<핀>`, 도메인 deps.

lock / requirements 생성 시 Airflow constraints 를 반영:

```
uv pip compile pyproject.toml -o requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt
```

생성된 `requirements.txt` 는 커스텀 이미지 빌드에, `uv.lock` (`uv sync --frozen`) 은 M1 호스트 직접 실행에 사용. 둘 다 동일 핀.

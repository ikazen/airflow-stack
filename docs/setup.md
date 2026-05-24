# Setup

인스턴스 / 워커 셋업. Phase 1~6 의 실행 절차.

전제: 열린 결정 확정 — O1·O2·O4 완료. O3 (M1 Tailscale 이름) 은 Phase 5 전.

## OCI 인프라 (Phase 1)

### VCN / 서브넷

```
컴파트먼트   main
VCN          main-vcn
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
| Boot | 125 GB | 75 GB |
| Public IP | reserved | 없음 |

### Tenancy

API key 정리. Cloud Guard / Notification / 알람 3종 / Budget 는 Phase 8 (검증 후) — `tasks.md`.

### DNS

`airflow.<your-domain>` A → ops-vm reserved IP. 옛 서브도메인 제거.

## 호스트 부트스트랩 (Phase 2)

`scripts/host-setup.sh`: swap, Docker Engine + Compose plugin, unattended-upgrades.

Tailscale 3 노드 가입. ACL: SSH tailnet 만, ops-vm 443/80 공인. MagicDNS ON.

## 컨트롤 플레인 (Phase 3, ops-vm)

### 커스텀 이미지 (L19)

공식 `apache/airflow:3.2.1` 에는 edge3 provider 도 도메인 deps 도 없음 → `infra/airflow.Dockerfile` 로 확장.

`requirements.txt` = `apache-airflow-providers-edge3==3.6.0` + `apache-airflow-providers-postgres` + 도메인 deps, constraints 핀 (아래 "의존성 핀"). 공식 이미지는 멀티아치 (arm64 포함) — ARM 그대로 빌드.

api-server·scheduler·dag-processor·워커 전부 이 이미지 사용.

### Compose 서비스

`infra/ops-vm/docker-compose.yml` 참조. 서비스 목록:

- `postgres:16` — DB `airflow`
- `airflow-init` — `db migrate` + `variables import` (아래 Phase 6 참조). 최초 1회
- `api-server` — `${OPS_TAILNET_IP}:8080:8080` bind (tailnet only, L20)
- `scheduler` / `dag-processor`
- `edge-worker-ops` — `--queues default,ops --concurrency 2`, `PYTHONPATH=/opt/airflow/lol-list`
- `caddy:2-alpine`

### 환경변수 핵심

`infra/ops-vm/.env.example` 참조. 핵심:

```
AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.EdgeExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
AIRFLOW__CORE__FERNET_KEY=<repo 외 보관>
AIRFLOW__API_AUTH__JWT_SECRET=<repo 외 보관>
OPS_TAILNET_IP=<oci-ops-tailnet-ip>
```

### DB 초기화 + admin

- airflow-init 서비스가 `db migrate` + `variables import` 자동 실행
- admin user 는 `AIRFLOW__SIMPLE_AUTH_MANAGER__USERS=admin` 으로 정의. 비번은 Airflow 가 자동 생성 → `simple_auth_manager_passwords.json.generated` 에서 읽어 로그인
- SimpleAuthManager 는 2FA·RBAC 없음. 노출 표면 늘릴 거면 `decisions.md` R3 (FabAuthManager) 재고

### Caddyfile

`infra/ops-vm/Caddyfile` 참조. 보안 헤더 (HSTS·X-Frame-Options·X-Content-Type-Options) + `/edge_worker/v1/*` 차단 + `api-server:8080` reverse proxy.

### 워커 경로 (L20)

api-server 8080 을 ops-vm 의 Tailscale interface 에만 bind. compose 의 `ports` 를 `${OPS_TAILNET_IP}:8080:8080` 로, ops-vm `.env` 에 `OPS_TAILNET_IP` (tailnet IP) 추가. 워커는 `http://<ops-vm-tailnet>:8080/edge_worker/v1` 로 직결. Caddy 는 같은 compose 네트워크 안에서 `api-server:8080` 으로 접근 (docker 내부 DNS) — 호스트 포트 매핑 무관. NSG + OS bind 두 겹 방어 (NSG 룰 실수 시에도 공인 IP 의 8080 닫힘).

### 시작

```
docker compose up -d
# https://airflow.<your-domain> → admin 로그인
# 비번 = simple_auth_manager_passwords.json.generated
```

## 안정 워커 (Phase 4, worker-vm)

```bash
git clone git@github.com:<your-repo>/airflow-stack.git ~/airflow-stack
git clone git@github.com:<your-repo>/lol-list.git ~/lol-list  # SSH deploy key 필요
```

`infra/worker-vm/docker-compose.yml`: `network_mode: host`, `--queues default --concurrency 4`, `PYTHONPATH=/opt/airflow/lol-list`.

env (`infra/worker-vm/.env`): `AIRFLOW__EDGE__API_URL=http://<ops-vm-tailnet>:8080/edge_worker/v1/rpcapi`, JWT, Fernet.

```
docker compose up -d
```

검증: UI Edge Workers 탭 healthy.

## M1 워커 (Phase 5, mac-server)

런타임: Colima + Docker 컨테이너 (`infra/mac-server/docker-compose.yml`).

```bash
brew install colima docker docker-compose
colima start --cpu 4 --memory 8 --vm-type vz

git clone git@github.com:<your-repo>/airflow-stack.git ~/airflow-stack
git clone git@github.com:<your-repo>/lol-list.git ~/lol-list  # SSH deploy key 필요
```

env (`infra/mac-server/.env`): JWT, Fernet, `LOL_LIST_PATH=~/lol-list`, `AIRFLOW__EDGE__API_URL`.

```
cd ~/airflow-stack && docker compose -f infra/mac-server/docker-compose.yml up -d
```

colima 자동 시동 (재부팅 시): `docs/runbook.md` LaunchAgent 절차 참조.

검증: UI Edge Workers 탭 — mac-server gpu+default queue healthy.

## lol-list 연동 (Phase 6)

### SSH deploy key

ops-vm / worker-vm / mac-server 각각 ed25519 키 생성 + GitHub lol-list repo 에 read-only deploy key 등록:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
# GitHub → lol-list → Settings → Deploy keys → Add (read only)
```

### airflow-variables.json

`infra/ops-vm/airflow-variables.json` 은 gitignored. `airflow-variables.json.example` 참고해 ops-vm 에 직접 작성. airflow-init 이 `docker compose up` 시 자동 import.

### DAG 구조

thin wrapper DAG (`dags/`) + 비즈니스 로직 (`~/lol-list`). `PYTHONPATH=/opt/airflow/lol-list` 로 연결. 상세: `docs/asset-model.md`.

## Secrets

- `.env` 커밋 금지. `.env.example` 만 (placeholder 값)
- 로컬 password manager / secrets vault 보관
- Fernet key 분실 시 암호화 값 복호화 불가 → 안전 보관 (단 Connection/Variable 미사용이면 영향 적음)

## 의존성 핀

`requirements.txt` 생성 시 Airflow constraints 반영:

```
uv pip compile requirements.in -o requirements.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt
```

# Setup

인스턴스 / 워커 셋업. Phase 1~5 의 실행 절차.

전제: 열린 결정 (O1 repo 이름, O3 도메인 라벨 등) 확정.

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

노드 간 통신은 Tailscale / 공인 HTTPS 위로. subnet 간 ingress 룰 불필요.

### 인스턴스

| | ops-vm | worker-vm |
|---|---|---|
| Shape | A1.Flex 2/12 GB | A1.Flex 2/12 GB |
| OS | Ubuntu 22.04 ARM | Ubuntu 22.04 ARM |
| Boot | 100 GB Bronze | 50 GB Bronze |
| Public IP | reserved | 없음 |

### Tenancy

API key 정리. Cloud Guard ON. Notification + 알람 3종 (CPU>80% 10분 / 인스턴스 not RUNNING / Boot vol >85%). Budget alert.

### DNS

`airflow.<your-domain>` A → ops-vm reserved IP. 옛 서브도메인 제거.

## 호스트 부트스트랩 (Phase 2)

`scripts/host-setup.sh`: swap, Docker Engine + Compose plugin, fail2ban, unattended-upgrades.

Tailscale 3 노드 가입. ACL: SSH tailnet 만, ops-vm 443/80 공인. MagicDNS ON.

## 컨트롤 플레인 (Phase 3, ops-vm)

### Compose 서비스

- `postgres:16-alpine` — DB `airflow`, Tailscale interface 또는 localhost 만 bind
- `apache/airflow:3.2.1` (ARM 확인, 미지원 시 자체 빌드) 로 3 service:
  - `api-server` / `scheduler` / `dag-processor`
- `caddy:2-alpine`

### 환경변수 핵심

```
AIRFLOW__CORE__EXECUTOR=airflow.providers.edge.executors.edge_executor.EdgeExecutor
  (정확 경로 3.2.1 docs 검증)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
AIRFLOW__CORE__FERNET_KEY=<repo 외 보관>
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.SimpleAuthManager
AIRFLOW__EDGE__API_URL=https://airflow.<your-domain>/edge_worker/v1
AIRFLOW__EDGE__JWT_SECRET=<repo 외 보관>
```

### Caddyfile

```
airflow.<your-domain> {
    reverse_proxy api-server:8080
}
```

### 시작

```
docker compose up -d
# https://airflow.<your-domain> → admin 로그인 (강한 비번, 가능하면 2FA)
```

## 안정 워커 (Phase 4, worker-vm)

```
git clone <new-repo> /opt/<repo>
cd /opt/<repo> && uv sync --frozen
```

`infra/worker-vm/docker-compose.yml`: `airflow edge worker --queues default --concurrency 4`. env: API URL, JWT, Fernet. volume: `/opt/<repo>:/opt/airflow/dags:ro`.

검증: UI Edge Workers 탭 healthy.

## M1 워커 (Phase 5)

```
brew install uv
git clone <new-repo> ~/Code/<repo>
cd ~/Code/<repo> && uv sync --frozen
```

컨테이너 vs 호스트: Docker Desktop on M1 부담 평가 후 결정. 호스트 직접이면 `uv add apache-airflow apache-airflow-providers-edge` (constraints 포함).

### LaunchAgent

`~/Library/LaunchAgents/<reverse-domain>.airflow-worker.plist`:
- Label: `<reverse-domain>.airflow-worker` (본인 도메인 reverse-DNS)
- WorkingDirectory: `~/Code/<repo>`
- EnvVars: `AIRFLOW__EDGE__API_URL=https://airflow.<your-domain>/edge_worker/v1`, JWT, Fernet
- ProgramArguments: `[uv, run, airflow, edge, worker, --queues, mac, --concurrency, 2]`
- KeepAlive / RunAtLoad: true
- Logs: `~/Library/Logs/airflow-worker-{out,err}.log`

```
launchctl load ~/Library/LaunchAgents/<reverse-domain>.airflow-worker.plist
```

검증: UI mac queue healthy. sleep/wake 자동 복귀.

## Secrets

- `.env` 커밋 금지. `.env.example` 만 (placeholder 값)
- 로컬 password manager / secrets vault 보관
- Fernet key 분실 시 모든 Connection/Variable 복호화 불가 → 안전 보관

## 의존성 핀

`pyproject.toml`: `apache-airflow==3.2.x`, `apache-airflow-providers-edge==<핀>`, `apache-airflow-providers-postgres==<핀>`, 도메인 deps. uv lock + constraints file 동시:

```
pip install ... -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt
```

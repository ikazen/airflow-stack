# Architecture

3 노드 분산. Edge Executor 기반.

## 토폴로지

```
                          인터넷
                            │ HTTPS (airflow.<your-domain>)
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  ops-vm  (OCI public, always-on, 2 OCPU / 12 GB)            │
│   Caddy · Postgres 16 · api-server · scheduler              │
│   dag-processor · Tailscale                                  │
└──────────────────────────────────────────────────────────────┘
        │ Tailscale (MagicDNS / ACL)
┌──────────────────────────────────────────────────────────────┐
│  worker-vm  (OCI private, always-on, 2 OCPU / 12 GB)        │
│   edge worker --queues default · uv venv · Tailscale         │
└──────────────────────────────────────────────────────────────┘
        │ Tailscale
┌──────────────────────────────────────────────────────────────┐
│  macbook  (M1, 가정 NAT, intermittent, 10-core / 32 GB)      │
│   edge worker --queues mac (launchd) · uv venv · Tailscale   │
└──────────────────────────────────────────────────────────────┘
```

## Edge Executor 의 핵심

- 워커 → ops-vm = **HTTPS long-poll 한 채널만**. DB / Redis 접근 0
- Task SDK 가 모든 상태를 api-server 경유
- NAT 뒤 M1 도 outbound 만으로 동작 (broker 없음)
- JWT 토큰: api-server 가 워커별 발행, env 로 보유

## Queue

| queue | 위치 | 라우팅 |
|---|---|---|
| `default` | worker-vm | 미지정 task 의 기본 |
| `mac` | M1 | `queue="mac"` 명시한 task. 가용성 가정 금지 |

## 네트워크 흐름

- 공인 ingress: ops-vm 443 → Caddy → api-server:8080. 80 → ACME redirect
- 워커 경로 (v1): Caddy 통한 공인 URL `https://airflow.<your-domain>/edge_worker/v1` (tailnet 직결은 internal cert 필요 — v2)
- SSH: 공개 폐지, Tailscale 만 + 본인 IP /32 fallback

## OCI 자원

| 항목 | ops-vm | worker-vm |
|---|---|---|
| Shape / OCPU / RAM | A1.Flex 2 / 12 GB | A1.Flex 2 / 12 GB |
| Boot Volume | 100 GB (Bronze) | 50 GB (Bronze) |
| Public IP | reserved | 없음 |
| Subnet | public | private |

합산 4 OCPU + 24 GB → A1.Flex 무료 한도 안.

## DAG processor 분리 (L15)

scheduler 와 다른 컨테이너로 격리. 파싱 오류가 scheduler 에 영향 안 줌. Airflow 3 권장.

워크로드 모델링과 lol-list 매핑은 `docs/asset-model.md`.

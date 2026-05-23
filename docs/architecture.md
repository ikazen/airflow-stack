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
│   edge worker --queues gpu,default (launchd) · Tailscale     │
└──────────────────────────────────────────────────────────────┘
```

## Edge Executor 의 핵심

- 워커 → ops-vm = **HTTPS long-poll 한 채널만**. DB / Redis 접근 0
- Task SDK 가 모든 상태를 api-server 경유
- NAT 뒤 M1 도 outbound 만으로 동작 (broker 없음)
- JWT: 공유 `EDGE__JWT_SECRET` (HMAC). 워커가 그 시크릿으로 자체 서명한 토큰 제시 — api-server 발행 토큰 아님

## Queue

| queue | 구독 worker | 라우팅 |
|---|---|---|
| `default` | worker-vm · ops-vm · mac-server | 미지정 task 의 기본 |
| `ops` | ops-vm | control-plane 작업 (예: deploy DAG) |
| `gpu` | mac-server | `queue="gpu"` 명시. GPU / Neural Engine 활용. M1 가용성 가정 금지 |

## 네트워크 흐름

- 공인 ingress: ops-vm 443 → Caddy → api-server:8080 (사람용 UI 만). 80 → ACME redirect
- 워커 경로: Tailscale 직결 `http://<ops-vm-tailnet>:8080/edge_worker/v1` (L20). Tailscale 가 암호화 채널이라 cert 불필요, edge API 는 공인 노출 안 함
- SSH: 공개 폐지, Tailscale 만 + 본인 IP /32 fallback

## OCI 자원

| 항목 | ops-vm | worker-vm |
|---|---|---|
| Shape / OCPU / RAM | A1.Flex 2 / 12 GB | A1.Flex 2 / 12 GB |
| Boot Volume | 125 GB | 75 GB |
| Public IP | reserved | 없음 |
| Subnet | public | private |

합산 4 OCPU + 24 GB → A1.Flex 무료 한도 안.

## DAG processor 분리 (L15)

scheduler 와 다른 컨테이너로 격리. 파싱 오류가 scheduler 에 영향 안 줌. Airflow 3 권장.

## 이미지 (L19)

api-server·scheduler·dag-processor·워커 모두 공식 `apache/airflow:3.2.x` 확장 커스텀 이미지 — edge3 provider·도메인 deps 가 공식 이미지엔 없음. 빌드는 `docs/setup.md`.

워크로드 모델링과 lol-list 매핑은 `docs/asset-model.md`.

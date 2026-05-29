# Architecture

Airflow 3.2.x workload. Edge Executor 기반. 인프라 (호스트·네트워크·OCI·Caddy·Postgres·Registry) 는 별도 repo `nexus-prime`.

## 서비스 배치

| 호스트 | airflow 서비스 |
|---|---|
| ops-vm | api-server / scheduler / dag-processor / edge-worker-ops (`ops` queue 전용) |
| worker-vm | edge-worker (`default` queue) |
| mac-server | edge-worker (`gpu,default` queue) |

호스트·네트워크 토폴로지·OCI 자원·Tailscale 메쉬는 `nexus-prime:docs/architecture.md`.

## Edge Executor 의 핵심

- 워커 → ops-vm = **HTTPS long-poll 한 채널만**. DB / Redis 접근 0
- Task SDK 가 모든 상태를 api-server 경유
- NAT 뒤 M1 도 outbound 만으로 동작 (broker 없음)
- JWT: 공유 `EDGE__JWT_SECRET` (HMAC). 워커가 그 시크릿으로 자체 서명한 토큰 제시 — api-server 발행 토큰 아님

## Queue

| queue | 구독 worker | 라우팅 |
|---|---|---|
| `default` | worker-vm · mac-server | 미지정 task 의 기본 |
| `ops` | ops-vm | control-plane 작업 전용 (ops-vm 은 `default` 미구독 — 컨트롤 플레인 자원 보호) |
| `gpu` | mac-server | `queue="gpu"` 명시. GPU / Neural Engine 활용. M1 가용성 가정 금지 |

## DAG processor 분리 (L15)

scheduler 와 다른 컨테이너로 격리. 파싱 오류가 scheduler 에 영향 안 줌. Airflow 3 권장.

## 이미지 (L19)

api-server·scheduler·dag-processor·워커 모두 공식 `apache/airflow:3.2.x` 확장 커스텀 이미지 — edge3 provider 가 공식 이미지엔 없음. 빌드는 `docs/setup.md`.

Phase 9 (계획) 에서 task 실행은 별도 image (`@task.docker`) 로 — `decisions.md` L24, `tasks.md` Phase 9.

## 인프라 의존 (nexus-prime 제공)

서비스 입장에서 본 결합점·내부 주소·발급 절차는 `nexus-prime:docs/dev-guide.md` 가 정본.

- `nexus` docker network (external) — airflow 서비스가 join (`networks: { nexus: { external: true } }`)
- `postgres:5432` — 공유 DB 안 `airflow` database (DB/user 발급은 dev-guide)
- `registry.internal` — task image registry (Phase 9 활용 예정). DNS 미작동 시 fallback `<ops-vm-tailnet>:5000`
- 워커 → api-server = Tailscale 직결 (cert 불필요, edge API 공인 노출 X)

airflow 는 nexus-prime 의 `compose/_hosts/` include 가 아니라 **별도 repo 의 자체 compose** 로 외부 `nexus` 네트워크에 join — dev-guide 의 "신규 서비스 추가 체크리스트" (nexus-prime 내부 서비스용) 와 구분.

워크로드 모델링과 lol-list 매핑은 `docs/asset-model.md`.

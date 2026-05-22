# Decisions

## 잠긴 결정

재고 거의 없음. 환경·요구가 근본적으로 바뀌지 않는 한 유지.

| # | 결정 | 근거 |
|---|---|---|
| L1 | Airflow 3.2.x (self-host, OSS) | 5년 Airflow 2 자산 + 3.0 진화 학습 |
| L2 | 옛 n8n 스택 폐기 | 잔존 가치 0 |
| L3 | ops-vm (공인) + worker-vm (사설) + M1 (intermittent) | 항상성·노출·자원 매핑 |
| L4 | Edge Executor (`apache-airflow-providers-edge3`) | Celery+broker 부담 0, NAT 뒤 워커 자연 동작 |
| L5 | Tailscale (MagicDNS + ACL) | NAT 뒤 outbound-only fit |
| L6 | Caddy + Let's Encrypt | 검증된 패턴 |
| L7 | Postgres 16 on ops-vm (`airflow` DB) | self-host 정공 |
| L8 | docker compose (호스트별 1) | k8s 도입 안 함 |
| L9 | M1 = launchd LaunchAgent | macOS 표준 |
| L10 | OCI Boot Volume Bronze 백업, 앱 백업 없음 | DAG/asset 은 코드. Connection/Variable 미사용 → 메타 DB disposable, 손실 시 재배포 |
| L11 | SSH 공개 폐지, 본인 IP /32 fallback | audit Critical 해소 |
| L13 | 코드 배포 = 전 호스트 git clone + git pull (v1) | 최소 운반 |
| L14 | Airflow 내장 auth + 단일 admin. Caddy TLS 만 | 단일 사용자 환경 |
| L15 | DAG processor 별도 컨테이너 | Airflow 3 권장 분리 |
| L16 | 공개 repo 정책 — placeholder 강제 | git history 영구 |
| L17 | 호스트네임 ops-vm / worker-vm / macbook | 역할 명시. 식별자 누출 없음 |
| L18 | OCI 자원 균등 — ops-vm·worker-vm 각 2 OCPU / 12 GB | A1.Flex 무료 한도 4+24 안. 컨트롤 플레인 peak 4 GB 추정 |
| L19 | 컨트롤 플레인·워커 = 커스텀 이미지 (`apache/airflow:3.2.x` + edge3 + 도메인 deps) | 공식 이미지에 edge3·도메인 deps 미포함 |
| L20 | 워커 → 컨트롤 플레인 = Tailscale 직결 HTTP (v1) | Tailscale 가 암호화 채널. cert 불필요, edge API 공개 노출 회피 |
| L21 | 공개 repo 이름 = `airflow-stack` | 로컬 디렉토리·README 와 일치, rename 불필요 |
| L22 | 공인 도메인 라벨 = `airflow` (`airflow.<your-domain>`) | 명확·직관 |

L12 는 R1 (워크로드 모델링) 으로 이동.

## 재고 가능 결정

각 결정 옆에 재고 트리거 와 마이그레이션 path 명시.

| # | 결정 / 현재 선택 | 재고 트리거 | 마이그레이션 |
|---|---|---|---|
| R1 | 워크로드 모델링 (구 L12) — lol-list v1 = 전통 `@dag` | derived data / 다른 소스 dependency 등장 | 점진적으로 `@asset` 도입 (`docs/asset-model.md`) |
| R2 | 코드 배포 — 수동 `git pull` + restart | 호스트 ≥4 또는 배포 빈도 주 ≥5 | GitHub Actions push → Airflow 3 DAG bundle |
| R3 | Auth manager — `SimpleAuthManager` (Airflow 3 기본) | 다중 사용자 / RBAC, 또는 UI 노출 표면 확대 | `FabAuthManager` 전환 |
| R4 | Triggerer — v1 스킵 | deferrable operator / async sensor 필요 | triggerer 컨테이너 추가 |

R3 주의: SimpleAuthManager 는 Apache 공식 문서가 "dev/test 전용, production 비권장" 으로 명시. 단일 사용자 + 공개 표면 최소화(L20 — edge API 비공개) 전제로 v1 채택. UI 노출 확대 시 R3 즉시 재고.

## 열린 결정 (답 필요)

| # | 질문 | 비고 |
|---|---|---|
| O3 | M1 Tailscale 이름 노출 | MagicDNS 기본 또는 별명. Phase 5 전까지 보류 |

O1·O2 → 잠긴 결정 (L21·L22). O4 (모니터링 시점) → Phase 7 검증 후로 결정, `tasks.md` Phase 8.

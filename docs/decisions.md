# Decisions

Airflow workload 결정. 인프라 결정은 `nexus-prime:docs/decisions.md`.

## 잠긴 결정

| # | 결정 | 근거 |
|---|---|---|
| L1 | Airflow 3.2.x (self-host, OSS) | 5년 Airflow 2 자산 + 3.0 진화 학습 |
| L2 | 옛 n8n 스택 폐기 | 잔존 가치 0 |
| L4 | Edge Executor (`apache-airflow-providers-edge3`) | Celery+broker 부담 0, NAT 뒤 워커 자연 동작 |
| L13 | ~~코드 배포 = 전 호스트 git clone + git pull (v1)~~ → L24·L25 로 대체 (2026-05-24) | 최소 운반 — 운용/실행 환경 분리·노드 환경 매트릭스·버전 정합성 불가로 폐기 |
| L14 | Airflow 내장 auth + 단일 admin. Caddy TLS 만 | 단일 사용자 환경 |
| L15 | DAG processor 별도 컨테이너 | Airflow 3 권장 분리 |
| L16 | 공개 repo 정책 — placeholder 강제 | git history 영구. nexus-prime L14 와 같음 |
| L19 | 컨트롤 플레인·워커 = 커스텀 이미지 (`apache/airflow:3.2.x` + edge3 + 도메인 deps) | 공식 이미지에 edge3·도메인 deps 미포함 |
| L21 | 공개 repo 이름 = `airflow-stack` | 로컬 디렉토리·README 와 일치, rename 불필요 |
| L23 | lol-list = `pip install` 가능 패키지 (`pyproject.toml` + dep 선언) | PYTHONPATH bind mount 트릭의 transitive deps 결합·버전 추적 불가·import 캐시 비결정 해소 |
| L24 | DAG task 실행 = `@task.docker` (별도 컨테이너, DooD). 워커 = thin runtime | 운용 (scheduler·worker) ↔ 실행 (task body) 환경 분리. 노드별 환경 매트릭스 = task image 단위로 표현. import 캐시 일관성 확보. DooD 보안 implication 은 신뢰 self-host 환경 전제 |
| L26 | image 태그 = sha-pinned, `force_pull=False` | k8s `IfNotPresent` 등가. immutable tag = 캐시 hit 안전, mutable tag 의 비결정성 회피. 노드별로 sha 당 1 회 pull |

## 재고 가능 결정

| # | 결정 / 현재 | 재고 트리거 | 마이그레이션 |
|---|---|---|---|
| R1 | 워크로드 모델링 (구 L12) — lol-list v1 = 전통 `@dag` | derived data / 다른 소스 dependency 등장 | 점진적으로 `@asset` 도입 (`docs/asset-model.md`) |
| R2 | ~~코드 배포 — 수동 `git pull` + restart~~ → L24·L25 로 해소 (2026-05-24) | — | DAG 파일은 git, task body 는 registry image (sha-pinned). 자동화는 GitHub Actions → registry push 로 자연 확장 |
| R3 | Auth manager — `SimpleAuthManager` (Airflow 3 기본) | 다중 사용자 / RBAC, 또는 UI 노출 표면 확대 | `FabAuthManager` 전환 |
| R4 | Triggerer — v1 스킵 | deferrable operator / async sensor 필요 | triggerer 컨테이너 추가 |

R3 주의: SimpleAuthManager 는 Apache 공식 문서가 "dev/test 전용, production 비권장" 으로 명시. 단일 사용자 + 공개 표면 최소화 (nexus-prime L11 — edge API 비공개) 전제로 v1 채택. UI 노출 확대 시 R3 즉시 재고.

## 인프라 layer 로 이주된 결정 (2026-05-24)

| 원 # | 내용 | 새 위치 |
|---|---|---|
| L3 | ops-vm + worker-vm + M1 (역할 분리) | nexus-prime L1 |
| L5 | Tailscale (MagicDNS + ACL) | nexus-prime L2 |
| L6 | Caddy + Let's Encrypt | nexus-prime L3 |
| L7 | Postgres 16 on ops-vm (공유 DB) | nexus-prime L4 |
| L8 | docker compose (호스트별 1) | nexus-prime L5 |
| L9 | M1 = launchd LaunchAgent | nexus-prime L6 |
| L10 | 백업 안 함 | nexus-prime L7 |
| L11 | SSH 공개 폐지, 본인 IP /32 fallback | nexus-prime L8 |
| L17 | 호스트네임 ops-vm / worker-vm / mac-server | nexus-prime L9 |
| L18 | OCI 자원 균등 (A1.Flex 2/12 GB × 2) | nexus-prime L10 |
| L20 | 워커 → ops-vm = Tailscale 직결 HTTP | nexus-prime L11 |
| L22 | 공인 도메인 라벨 = `airflow` | nexus-prime L12 |
| L25 | self-hosted `registry:2` on ops-vm | nexus-prime L13 |

L12 는 R1 (워크로드 모델링) 으로 이동 (이전 정리).

## 열린 결정 (답 필요)

(없음 — Phase 5 의 O3 는 처리됨, O4 는 Phase 7 검증 후)

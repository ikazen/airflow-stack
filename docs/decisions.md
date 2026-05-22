# Decisions

## 잠긴 결정

재고 거의 없음. 환경·요구가 근본적으로 바뀌지 않는 한 유지.

| # | 결정 | 근거 |
|---|---|---|
| L1 | Airflow 3.2.x (self-host, OSS) | 5년 Airflow 2 자산 + 3.0 진화 학습 |
| L2 | 옛 n8n 스택 폐기 | 잔존 가치 0 |
| L3 | ops-vm (공인) + worker-vm (사설) + M1 (intermittent) | 항상성·노출·자원 매핑 |
| L4 | Edge Executor | Celery+broker 부담 0, NAT 뒤 워커 자연 동작 |
| L5 | Tailscale (MagicDNS + ACL) | NAT 뒤 outbound-only fit |
| L6 | Caddy + Let's Encrypt | 검증된 패턴 |
| L7 | Postgres 16 on ops-vm (`airflow` DB) | self-host 정공 |
| L8 | docker compose (호스트별 1) | k8s 도입 안 함 |
| L9 | M1 = launchd LaunchAgent | macOS 표준 |
| L10 | OCI Boot Volume Bronze 백업, 앱 백업 없음 | DAG/asset 은 코드, 메타 손실 시 재배포 복구 |
| L11 | SSH 공개 폐지, 본인 IP /32 fallback | audit Critical 해소 |
| L13 | 코드 배포 = 전 호스트 git clone + uv venv (v1) | 최소 운반 |
| L14 | Airflow 내장 auth + 단일 admin. Caddy TLS 만 | 단일 사용자 환경 |
| L15 | DAG processor 별도 컨테이너 | Airflow 3 권장 분리 |
| L16 | 공개 repo 정책 — placeholder 강제 | git history 영구 |

## 재고 가능 결정

각 결정 옆에 **재고 트리거** 와 **마이그레이션 path** 명시.

| 결정 | 현재 선택 | 재고 트리거 | 마이그레이션 |
|---|---|---|---|
| 워크로드 모델링 (구 L12) | 워크로드별 best practice 선택. lol-list v1 = 전통 `@dag` | derived data / 다른 소스 dependency 등장 시 lineage 가치 발생 | 점진적으로 `@asset` 도입 (`docs/asset-model.md`) |
| 코드 배포 디테일 (O5) | 수동 `git pull` + restart | 호스트 수 ≥4 또는 배포 빈도 주 ≥5 | (b) GitHub Actions push → (c) Airflow 3 DAG bundle |
| Auth manager (O8) | `simple_auth_manager` 예정 | 다중 사용자 / RBAC 필요 | `FabAuthManager` 전환 |
| Triggerer (O6) | v1 스킵 | deferrable operator / async sensor 필요 | triggerer 컨테이너 추가 |

## 열린 결정 (답 필요)

| # | 질문 | 비고 |
|---|---|---|
| O1 | 새 repo 이름 | 옛 식별자 / n8n 잔재 금지 |
| O2 | 노드 호스트네임 | 후보: `ops-vm` + `worker-vm` + `macbook` |
| O3 | 공인 도메인 라벨 | `airflow.<your-domain>` 권장 |
| O4 | OCI 자원 배분 | ops-vm 2 OCPU/12GB + worker-vm 2 OCPU/12GB (균등 — A1.Flex 무료 한도 4+24 안). Airflow 컨트롤 플레인 peak 4 GB 안쪽 추정으로 충분 |
| O9 | M1 Tailscale 이름 노출 | MagicDNS 기본 또는 별명 |
| O10 | Cloud Guard / 알람 시점 | Phase 1 끝 일괄 (잠정) |

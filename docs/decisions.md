# Decisions

Airflow workload 결정. 인프라 결정은 `nexus-prime:docs/decisions.md`.

## 잠긴 결정

| # | 결정 | 근거 |
|---|---|---|
| L1 | Airflow 3.2.x (self-host, OSS) | 5년 Airflow 2 자산 + 3.0 진화 학습 |
| L4 | Edge Executor (`apache-airflow-providers-edge3`) | Celery+broker 부담 0, NAT 뒤 워커 자연 동작 |
| L13 | ~~코드 배포 = 전 호스트 git clone + git pull (v1)~~ → L24·L25 로 대체 (2026-05-24) | 최소 운반 — 운용/실행 환경 분리·노드 환경 매트릭스·버전 정합성 불가로 폐기 |
| L14 | Airflow 내장 auth + 단일 admin. Caddy TLS 만 | 단일 사용자 환경 |
| L15 | DAG processor 별도 컨테이너 | Airflow 3 권장 분리 |
| L16 | 공개 repo 정책 — placeholder 강제 | git history 영구. nexus-prime L14 와 같음 |
| L19 | 컨트롤 플레인·워커 = 커스텀 이미지 (`apache/airflow:3.2.x` + edge3) | 공식 이미지에 edge3 미포함. 도메인 deps 는 lol-list 제거로 없음 (2026-05-30) |
| L21 | 공개 repo 이름 = `airflow-stack` | 로컬 디렉토리·README 와 일치, rename 불필요 |
| L24 | **워크로드 task 의 기본 = `@task.docker`** (별도 컨테이너, DooD). 워커 = thin runtime | 운용 (scheduler·worker) ↔ 실행 (task body) 환경 분리. 라이브러리·숨길 로직은 task 이미지에. 노드별 환경 매트릭스 = task image 단위. DooD 보안 implication 은 신뢰 self-host 전제 (2026-05-30 표준 확정) |
| L26 | task image 태그 = sha-pinned, `force_pull=False` | k8s `IfNotPresent` 등가. immutable tag = 캐시 hit 안전, mutable tag 비결정성 회피. 노드별 sha 당 1 회 pull |
| L27 | DAG 배포 = **GitDagBundle** (`apache-airflow-providers-git`), public repo `repo_url` 직접 (connection 없음) | Airflow 3 네이티브 DAG 분배 — dag-processor·워커가 repo 에서 `dags/` 직접 fetch. `git push` 가 곧 배포 (호스트 per-node pull 불필요). DAG Versioning(DagRun↔commit pin) 활성. Connection 미사용 = 메타 DB disposable (L10) 유지 |
| L28 | **ops 큐 = privileged 인프라 유지보수 전용**. `docker.sock` (호스트 docker root) 을 ops-vm 워커에 마운트 → 일반 워크로드 라우팅 금지로 blast radius 한정. registry retention+GC / build cache prune 을 systemd 타이머 → Airflow DAG 로 이전 (UI 가시성·재시도 확보). | nexus-prime `decisions.md` L21, `review-2026-06-18.md` F5 |

## 재고 가능 결정

| # | 결정 / 현재 | 재고 트리거 | 마이그레이션 |
|---|---|---|---|
| R1 | ~~워크로드 모델링 — lol-list v1 = 전통 `@dag`~~ → lol-list 제거로 무효 (2026-05-30). 모델링 선택 기준은 CLAUDE.md "워크로드 모델링" | 차기 도메인 워크로드 도입 시 재적용 | — |
| R2 | 코드 배포 — DAG 파일 = L27 (GitDagBundle), task body = L24 (`@task.docker` image, sha-pinned) | 둘 다 확정 (2026-05-30) | task image 빌드·push 자동화는 GitHub Actions → `registry.internal` 로 자연 확장 |
| R3 | Auth manager — `SimpleAuthManager` (Airflow 3 기본) | 다중 사용자 / RBAC, 또는 UI 노출 표면 확대 | `FabAuthManager` 전환 |
| R4 | Triggerer — ops-vm 컨테이너로 추가 (2026-05-30) | 향후 deferrable 사용 가능성 | — |
| R5 | **T-shirt sizing — 노드 내 워커 분리 (default/big)** (2026-06-06). edge3 concurrency 는 워커 단위 단일 값·per-queue 설정 없음 → 사이즈별 cap 차등은 워커 프로세스 분리로만. worker-vm: `default`(c=2)/`big`(c=1). mac-server: `default`(c=8)/`big`=`gpu,big`(c=4, GPU=heavy 라 묶음). 워커 이름 `--edge-hostname` distinct | task 리소스 footprint 변화·노드 RAM 압박·사이즈 티어 추가(예: medium) 필요 시 | concurrency 값 조정 / 큐 추가. cap=admission 일 뿐, 실제 상한은 `@task.docker mem_limit`/`cpus` 세트 |

R3 주의: SimpleAuthManager 는 Apache 공식 문서가 "dev/test 전용, production 비권장" 으로 명시. 단일 사용자 + 공개 표면 최소화 (nexus-prime L11 — edge API 비공개) 전제로 v1 채택. UI 노출 확대 시 R3 즉시 재고.


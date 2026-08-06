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
| L28 | **ops-vm 큐(구 `ops`, 2026-07-18 워커 이름 통일로 rename) = privileged 인프라 유지보수 전용**. `docker.sock` (호스트 docker root) 을 ops-vm 워커에 마운트 → 일반 워크로드 라우팅 금지로 blast radius 한정. registry retention+GC / build cache prune 을 systemd 타이머 → Airflow DAG 로 이전 (UI 가시성·재시도 확보). | nexus-prime `decisions.md` L21, `review-2026-06-18.md` F5 |
| L29 | **L28 확장 — ops-vm 큐를 "범용 워크로드 이미지 빌드+push"에도 재사용**(2026-07-14, issue #2). `dags/lib/image_deploy.py`가 여러 repo의 `git clone(scratch dir)+docker build+push`를 공용 헬퍼로 제공, repo별 DAG는 이 헬퍼를 파라미터만 바꿔 호출. reflexion-rondo가 첫 사용처(daemon+task 이미지). | 신규 write credential이 전혀 필요 없다는 게 핵심 근거 — clone은 읽기(public repo 무인증, private는 공유 read-only PAT), registry.internal:5000은 무인증. repo마다 상주 체크아웃+전용 SSH 키를 만드는 대안(daily_claude_ping.py 패턴)보다 기존 docker.sock 재사용이 repo 수가 늘어날 때 유지보수 부담이 작다. L28의 "일반 워크로드 라우팅 금지" 문구는 GC/prune 같은 인프라 유지보수만 염두에 뒀던 것이라 이 확장은 그 원칙을 의도적으로 넓히는 것 — 향후 이 큐에 또 다른 종류의 작업을 얹을 때 "빌드/push까지는 허용, 그 이상은 안 됨"이라는 기준으로 재적용 |
| L30 | **lck-pics(lol-list, private repo) data-sync 이미지 빌드도 `data_sync_deploy` DAG로 이관**(2026-07-20, issue #24). L29 `private_pat_var` 경로 첫 실사용 — read-only PAT 1개(`lck_pics_repo_pat` Variable)만 추가, credential 관리 부담이 예상보다 가벼워 R2가 우려한 "GH Actions 재검토" 트리거는 아직 미발동. 기존 M1 mac `scripts/build-and-push.sh` 수동 빌드+수동 Variable bump는 registry/인프라 장애 시 긴급 fallback으로 격하 | KESPA Cup 2026 LIVE 표시 버그(lck-pics#4 후속)의 근본 원인이 "머지됐지만 이미지 재배포 누락"이었다 — 배포를 사람 기억 대신 Airflow UI 트리거 한 번으로 좁혀 재발 방지 |

## 재고 가능 결정

| # | 결정 / 현재 | 재고 트리거 | 마이그레이션 |
|---|---|---|---|
| R1 | ~~워크로드 모델링 — lol-list v1 = 전통 `@dag`~~ → lol-list 제거로 무효 (2026-05-30). 모델링 선택 기준은 CLAUDE.md "워크로드 모델링" | 차기 도메인 워크로드 도입 시 재적용 | — |
| R2 | 코드 배포 — DAG 파일 = L27 (GitDagBundle), task body = L24 (`@task.docker` image, sha-pinned) | 둘 다 확정 (2026-05-30) | task image 빌드·push 자동화는 GitHub Actions → `registry.internal` 로 자연 확장 — **[2026-07 amend]** 실제 1차 구현(reflexion-rondo, L29)은 GitHub Actions 대신 기존 ops-vm 큐 docker.sock 재사용으로 감. GH Actions는 tailnet 밖 runner라 `registry.internal:5000` 도달 경로(Tailscale action 등)를 새로 만들어야 하는 반면, ops-vm 큐는 이미 존재하는 capability라 새 인프라 없이 확장 가능했음. **[2026-07-20 재amend]** private repo(lol-list) 편입(L30)으로 트리거 실현 — PAT 1개 추가로 충분해 credential 부담이 가볍다고 판정, GH Actions 재검토는 보류. PAT 수가 늘어나 관리가 무거워지면 그때 다시 검토 |
| R3 | Auth manager — `SimpleAuthManager` (Airflow 3 기본) | 다중 사용자 / RBAC, 또는 UI 노출 표면 확대 | `FabAuthManager` 전환 |
| R4 | Triggerer — ops-vm 컨테이너로 추가 (2026-05-30) | 향후 deferrable 사용 가능성 | — |
| R5 | **T-shirt sizing — 노드 내 워커 분리 (default/big)** (2026-06-06). edge3 concurrency 는 워커 단위 단일 값·per-queue 설정 없음 → 사이즈별 cap 차등은 워커 프로세스 분리로만. worker-vm: `default`(c=2)/`big`(c=1). mac-server: `default`(c=8)/`big`=`gpu,big`(c=2, GPU=heavy 라 묶음). 워커 이름 `--edge-hostname` distinct | task 리소스 footprint 변화·노드 RAM 압박·사이즈 티어 추가(예: medium) 필요 시 | concurrency 값 조정 / 큐 추가. cap=admission 일 뿐, 실제 상한은 `@task.docker mem_limit`/`cpus` 세트 |

R3 주의: SimpleAuthManager 는 Apache 공식 문서가 "dev/test 전용, production 비권장" 으로 명시. 단일 사용자 + 공개 표면 최소화 (nexus-prime L11 — edge API 비공개) 전제로 v1 채택. UI 노출 확대 시 R3 즉시 재고.


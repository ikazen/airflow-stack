# airflow-stack

개인용 Airflow 3.2.x self-host **워크로드** repo. 호스트·네트워크·Caddy·Postgres·Registry 등 인프라 layer 는 별도 repo `nexus-prime` (이 repo 는 그 위 `nexus` 네트워크에 join). 서비스 관점 통합 결합점은 `nexus-prime:docs/dev-guide.md`.

배치: OCI ARM 2대 (ops-vm 컨트롤 플레인 + worker-vm 안정 워커) + M1 (mac-server burst 워커). Edge Executor 기반.

첫 워크로드: lol-list ETL 3종 (matches / liquipedia 보강 / 일별 강제 재실행).

## 공개 repo 정책

도메인·IP·옛 식별자 평문 노출 금지. 자세한 컨벤션은 `CLAUDE.md` 참조.

## 폴더 구조

```
docs/                  설계 / 결정 / 운영 문서
  architecture.md      서비스 배치 + Edge Executor + 인프라 의존
  decisions.md         잠긴 결정 / 재고 가능 / 열린 결정
  tasks.md             Phase 진행 (현재 Phase 6 완료, Phase 9 계획)
  setup.md             셋업 절차
  runbook.md           정상 운영 절차
  troubleshooting.md   문제 진단·해결 사례
  asset-model.md       워크로드 모델링 가이드
  airflow3-learnings.md  3.x 신기능 학습 노트
infra/                 호스트별 compose + airflow.Dockerfile + .env.example
  ops-vm/              컨트롤 플레인 (api-server/scheduler/dag-processor/edge-worker-ops)
  worker-vm/           안정 워커
  mac-server/          M1 burst 워커
dags/                  DAG entry (sync_matches / sync_liquipedia / sync_lol_meta 등)
```

## 진입점

- 컨벤션: `CLAUDE.md`
- 결정 / 진행: `docs/decisions.md`, `docs/tasks.md`
- 셋업: `docs/setup.md`

## 현재 상태

Phase 6 완료 — lol-list ETL 3종 엔드투엔드 검증. 인프라 layer 는 `nexus-prime` 으로 분리 완료. 다음: Phase 9 (실행 환경 격리 — `@task.docker` + lol-list 패키지화). 세부는 `docs/tasks.md`.

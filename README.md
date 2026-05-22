# airflow-stack

개인용 Airflow 3.2.x self-host. OCI ARM 2대 (컨트롤 플레인 + 안정 워커) + M1 (burst 워커). Edge Executor 기반.

첫 워크로드: lol-list ETL 3종 (matches / liquipedia 보강 / 일별 강제 재실행).

## 공개 repo 정책

도메인·IP·옛 식별자 평문 노출 금지. 자세한 컨벤션은 `CLAUDE.md` 참조.

## 폴더 구조 (계획)

```
docs/                  설계 / 결정 / 운영 문서
  architecture.md      토폴로지 + 컴포넌트
  decisions.md         잠긴 결정 / 재고 가능 / 열린 결정
  tasks.md             Phase 0~7 진행
  setup.md             셋업 절차
  spec.md              DB 스키마·API 스펙 (Phase 6+)
  runbook.md           정상 운영 절차 (stub)
  troubleshooting.md   문제 진단·해결 사례 (stub)
  asset-model.md       워크로드 모델링 가이드
  airflow3-learnings.md  3.x 신기능 학습 노트
infra/                 (Phase 3+) compose / Caddyfile / .env.example
dags/                  (Phase 3+) DAG entry
src/                   (Phase 6+) collectors 도메인 코드
scripts/               (Phase 2+) host-setup 등
```

## 진입점

- 컨벤션: `CLAUDE.md`
- 결정 / 진행: `docs/decisions.md`, `docs/tasks.md`
- 셋업: `docs/setup.md`

## 현재 상태

Phase 0 진행 중. 옛 OCI 자산 정리 완료, 수동 항목(DNS·repo archive) 남음.

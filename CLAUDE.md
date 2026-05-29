# CLAUDE.md

Airflow 3.2.x self-host. Claude 세션 컨벤션. 사용자 글로벌 `~/.claude/CLAUDE.md` 위에 얹힘.

## 공개 repo 정책 (최우선)

코드·문서·커밋·주석 어디서도 평문 노출 금지: 사용자 도메인 / 옛 서브도메인 / 옛 repo 식별자 (한글·영문 변종) / 공인 IP / 본인 IP / tailnet 실제 이름 / 사용자 home 경로. 전부 placeholder (`<your-domain>`, `<previous-repo>`, `<tailnet>.ts.net`, ...).

예외: RFC1918 사설 IP (`10.0.0.0/16` 등) 만 OK.

정확한 옛 식별자 grep 목록은 **repo 외 메모리 보관** — 정책 문서가 자기 정책 안 어기게. 새 파일 작성 / 수정 후 메모리의 패턴으로 grep, 매치 0 이어야 함.

사용자가 알려주는 실제 값은 받아 적지 말고 placeholder 로만. 실제 값은 `.env` / 로컬 secrets / Claude 메모리에만.

## 워크로드 모델링 — 상황별 선택, 도그마 없음

전통 `@dag` 와 Data Asset (`@asset`) 둘 다 자유롭게. 신호로 판단:

- **전통 DAG**: 시간 cron 본질, 단일 잡, dependency 적은 ETL
- **Data Asset**: 여러 데이터의 lineage 가 운영 직관, downstream 이 dep 으로 굴러감
- **외부 polling / async sensor**: 전통 `@dag` + triggerer

Airflow 3 채택의 본 가치는 Edge Executor + Task SDK + DAG Versioning 이지 모델링 패러다임 강제가 아님.

## Edge Executor 사고

워커는 DB 안 침. Task SDK 가 모든 상태를 api-server 경유. 2.x 의 "task 안 `Variable.get()` → DB 직접" 패턴 금지. task callable 은 모든 워커 호스트에 import 가능해야 함.

## Queue

- `default` (worker-vm) — 미지정 task 의 기본
- `gpu` (M1, intermittent) — GPU / Neural Engine 필요 시 명시 지정. M1 가용성 가정 금지. mac-server 는 `gpu,default` 둘 다 구독 (가용 시 default task 도 흡수)

## 네트워크

노드 간 = Tailscale. 공인 노출 = Caddy 뒤 UI/API 한 군데. SSH 공개 폐지, 본인 IP /32 fallback.

## 코드 배포 (v1)

전 호스트에 git clone (코드 운반). 런타임 = edge3 포함 커스텀 이미지 컨테이너 (M1 은 Phase 5 결정). 변경 = 각 호스트 `git pull` + 영향 컨테이너 / launchd restart.

## 의존성

`apache-airflow==3.2.x` + provider 핀. constraints file + uv lock.

## 문서화

사용자 글로벌 CLAUDE.md 따름. 한 문서 1~2 스크롤. 코드 중복 설명 금지 → 파일 링크.

## 의심어

"n8n" 등장 시 의심. 옛 repo prefix 발견 시 즉시 placeholder.

## 진입점

- 결정 / 진행: `docs/decisions.md`, `docs/tasks.md`
- 셋업 절차: `docs/setup.md`
- 운영 문제: `docs/troubleshooting.md`

# 워크로드 모델링 가이드

전통 `@dag` 와 Data Asset (`@asset`) 의 선택 가이드. CLAUDE.md 의 "워크로드 모델링" 섹션 보강.

## 선택 기준

| 신호 | 권장 |
|---|---|
| 시간 cron 본질, 단일 잡 (외부 → DB UPSERT), dependency 적음 | 전통 `@dag` |
| 여러 데이터의 lineage 가 운영 핵심 질문, downstream 이 dep 으로 굴러감 | `@asset` |
| 외부 polling, async sensor, deferrable | 전통 `@dag` + triggerer |

도그마 없음. 같은 repo 에서 둘 다 쓰는 게 정공. 신기능 체득 동기로 asset 강제 금지.

## lol-list 결정 — v1 = 전통 `@dag`

옛 n8n 시절 (KST):

| 잡 | cron | 동작 |
|---|---|---|
| sync-matches | `*/10 * * * *` | matches UPSERT |
| sync-liquipedia | `*/15 * * * *` | matches 의 liquipedia 보강 UPSERT |
| sync-meta | `0 0 * * *` | leagues + matches + liquipedia 강제 재실행 |

전통 DAG 채택 근거:
- 외부 → Supabase UPSERT 의 단순 흐름. dependency 거의 없음
- 옛 n8n 잡 3개 → DAG 3개 매핑 직관, 이관 부담 최소
- UPSERT 멱등 → asset 의 "state" 추상화 가치 낮음
- Airflow 3 의 진짜 가치 (Edge Executor / Task SDK / DAG Versioning) 는 모델링과 독립

**Asset 도입 트리거** (재고 시점):
- lol-list 위에 derived data 추가 (통계 / 가공 / 외부 노출)
- 다른 데이터 소스 추가로 cross-source dependency 발생
- "이 데이터 지금 어떤 상태?" 가 운영 핵심 질문이 됨

## 자격증명 — Airflow Connection/Variable 미사용

collectors 는 Supabase 등 자격증명을 자체 관리 (`.env` / 로컬 설정). Airflow Connection/Variable 을 안 씀 → 메타 DB 에 코드 아닌 상태가 없음 → 메타 DB disposable (`decisions.md` L10). UI 로 Connection 을 만들기 시작하면 L10 백업 전략 재고.

## 코드 스케치 (v1)

```python
# dags/lol_list_matches.py
import pendulum
from airflow.sdk import dag, task

@dag(
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
)
def sync_matches():
    @task
    def run():
        from collectors import sync_matches
        return sync_matches.main()
    run()

sync_matches()
```

- cron 은 `start_date` 의 timezone (KST) 에서 해석됨. `@dag` 에 `timetable_kwargs` 같은 인자는 없음 — timezone 은 tz-aware `start_date` 로 (또는 `CronTriggerTimetable(timezone=...)`)
- `@dag` 함수는 모듈 끝에서 호출(`sync_matches()`)해야 dag-processor 가 발견
- `retries` 필수: 외부 사이트 호출이라 transient 실패 대비. 특히 `queue="mac"` task 는 M1 intermittent → retry 없으면 M1 sleep 중 그냥 실패

`sync_liquipedia`, `sync_meta` 동일 패턴. `sync_meta` 는 일별 단일 task 로 3 종 강제 호출.

## 폴더 배치

```
dags/
  lol_list_matches.py
  lol_list_liquipedia.py
  lol_list_meta.py
src/collectors/
  sync_matches.py
  sync_liquipedia.py
  sync_meta.py
  db.py
```

dag-processor 는 `dags/` 읽음. `from collectors import ...` 는 `@task` 콜러블 안 — 파싱 시점엔 실행 안 됨 (워커 실행 시점에만 import). 워커는 `src/` 를 `PYTHONPATH` 에 두고 같은 repo 를 호스트에 git clone 으로 보유.

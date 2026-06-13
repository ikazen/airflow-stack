"""reflexion-rondo 자동 Kaggle 제출 DAG.

매일 KST 06:00 실행. 최근 24h 내 cycle이 돈 대회 중
직전 제출 이후 best CV가 개선된 대회만 daemon API를 통해 제출한다.

daemon API: http://rondo-api.internal (하드코딩: _RONDO_API_URL)
"""
from __future__ import annotations

import json
import urllib.request
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

_RONDO_API_URL = "http://rondo-api.internal"


@dag(
    dag_id="reflexion_rondo_autosubmit",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["rondo"],
)
def reflexion_rondo_autosubmit() -> None:
    @task(queue="ops", retries=1, execution_timeout=timedelta(minutes=5))
    def trigger_auto_submit() -> None:
        url = f"{_RONDO_API_URL}/api/submissions/auto"
        payload = json.dumps({"window_hours": 24}).encode()

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read())

        submitted = body.get("submitted", [])
        skipped = body.get("skipped", [])
        print(f"[autosubmit] submitted={len(submitted)} skipped={len(skipped)}")
        for s in submitted:
            print(f"  + {s['competition']} ({s['slug']}) attempt={s['attempt_id'][:8]} sid={s['submission_id'][:8]}")
        for s in skipped:
            print(f"  - {s['competition']} reason={s['reason']}")

    trigger_auto_submit()


reflexion_rondo_autosubmit()

"""reflexion-rondo 자동 Kaggle 제출 + 폴링 DAG.

매일 KST 06:00 실행.
1. trigger_auto_submit: 최근 24h 내 cycle이 돈 대회 중 best CV 개선분만 daemon API로 제출.
2. poll_submissions: 제출된 id들을 /refresh로 polling — 전부 terminal이거나 ~30분까지.

daemon API: http://rondo-daemon:8000 (nexus 서비스명 직결, ops-vm edge-worker 전용)
"""
from __future__ import annotations

import json
import time
import urllib.request
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

_RONDO_API_URL = "http://rondo-daemon:8000"
_TERMINAL = frozenset({"complete", "error", "invalid"})
_POLL_INTERVAL_SEC = 20
_POLL_DEADLINE_SEC = 30 * 60  # 30분


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
    def trigger_auto_submit() -> list[str]:
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

        return [s["submission_id"] for s in submitted]

    @task(queue="ops", execution_timeout=timedelta(minutes=35))
    def poll_submissions(ids: list[str]) -> None:
        if not ids:
            print("[poll] no submissions to poll")
            return

        pending = set(ids)
        deadline = time.monotonic() + _POLL_DEADLINE_SEC

        while pending and time.monotonic() < deadline:
            time.sleep(_POLL_INTERVAL_SEC)
            still_pending: set[str] = set()

            for sid in pending:
                url = f"{_RONDO_API_URL}/api/submissions/{sid}/refresh"
                req = urllib.request.Request(url, method="POST")
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        rec = json.loads(resp.read())
                except Exception as exc:
                    print(f"  [warn] {sid[:8]} refresh error: {exc}")
                    still_pending.add(sid)
                    continue

                status = rec.get("status", "")
                lb = rec.get("lb_score")
                if status in _TERMINAL:
                    print(f"  [{status}] {sid[:8]} lb={lb}")
                else:
                    still_pending.add(sid)

            pending = still_pending

        if pending:
            print(f"[poll] deadline reached, still pending: {[s[:8] for s in pending]}")

    ids = trigger_auto_submit()
    poll_submissions(ids)


reflexion_rondo_autosubmit()

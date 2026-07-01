"""DAG run 실패 알람 — on_failure_callback 공유 유틸.

scheduler/dag-processor 프로세스에서 실행되므로 표준 라이브러리만 사용
(task image 의존성과 분리). 알람 전송 자체가 실패해도 DAG run 상태에
영향을 주면 안 되므로 예외를 전부 삼킨다.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"


def notify_discord_on_failure(context: dict[str, Any]) -> None:
    try:
        webhook_url = os.environ.get(_WEBHOOK_ENV)
        if not webhook_url:
            log.warning("%s not set — skipping discord alert", _WEBHOOK_ENV)
            return

        dag_run = context.get("dag_run")
        dag_id = getattr(dag_run, "dag_id", None) or context.get("dag", {}).get("dag_id", "?")
        run_id = getattr(dag_run, "run_id", "?")
        logical_date = getattr(dag_run, "logical_date", None) or context.get("logical_date")
        reason = context.get("reason", "")

        content = (
            f":red_circle: **Airflow DAG 실패**\n"
            f"dag=`{dag_id}` run=`{run_id}`\n"
            f"logical_date=`{logical_date}`\n"
            f"reason=`{reason}`"
        )
        payload = json.dumps({"content": content}).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception:
        log.exception("discord alert send failed")

"""매일 KST 18:00 mac-server 호스트의 claude CLI 에 "ㅎㅇ" 전송.

claude 는 mac-server 호스트 사용자 세션에 인증돼 있어 컨테이너(ops-vm edge-worker)에서
직접 실행 불가 — nexus-prime 의 claude-bridge(launchd, tailnet IP 바인드)를 HTTP 로 호출.
브리지 URL/토큰은 ops-vm edge-worker `.env` 를 통해 os.environ 으로 주입 (repo 하드코딩 금지).
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from lib.alert import notify_discord_on_failure


@dag(
    dag_id="daily_claude_ping",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["misc"],
    on_failure_callback=notify_discord_on_failure,
)
def daily_claude_ping() -> None:
    # queue="ops": ops-vm edge-worker 가 env_file(.env) 로 브리지 URL/토큰을 os.environ 에 가짐
    # retries: mac sleep/wake 로 브리지가 일시 부재할 수 있음 (daily_meta 와 동일 논리)
    @task(queue="ops", retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=3))
    def send_ping() -> None:
        url = os.environ["CLAUDE_BRIDGE_URL"]
        token = os.environ["CLAUDE_BRIDGE_TOKEN"]
        payload = json.dumps({"msg": "ㅎㅇ"}).encode()

        req = urllib.request.Request(
            f"{url}/ping",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=130) as resp:
            body = json.loads(resp.read())

        if not body.get("ok"):
            raise RuntimeError(
                f"claude bridge rc={body.get('returncode')} stderr={body.get('stderr', '')[:500]}"
            )
        print(f"[claude] {body.get('stdout', '').strip()}")

    send_ping()


daily_claude_ping()

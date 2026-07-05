"""매일 KST 12:59/13:00/18:00/18:01 mac-server 호스트의 claude CLI 에 "ㅎㅇ" 전송.

claude 는 mac-server 호스트 사용자의 macOS 로그인 키체인에 인증돼 있다. 키체인은
SSH 로그인(PAM 인증)을 거친 세션에서만 언락되고 launchd 등 데몬 프로세스에서는
접근 불가 — 상주 서비스(HTTP 브리지 등)로는 도달 불가능하다는 게 실측으로 확인됨.
따라서 매 실행마다 실제 SSH 인증을 거치는 이 방식이 유일하게 신뢰 가능한 경로.

mac-server 의 authorized_keys 에 이 태스크 전용 키를 forced command 로 등록
(`command="claude -p ㅎㅇ",restrict`) — 이 키로는 claude ping 외 아무 것도 실행 불가.
개인키는 ops-vm edge-worker `.env` 에 base64 로 보관, 태스크 실행 시점에만 임시
파일로 복원 후 사용·즉시 삭제.
"""
from __future__ import annotations

import base64
import os
import stat
import subprocess
import tempfile
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from lib.alert import notify_discord_on_failure


@dag(
    dag_id="daily_claude_ping",
    # 12:59/13:00, 18:00/18:01 더블탭 — 단일 cron 으론 교차곱 문제로 표현 불가
    schedule=MultipleCronTriggerTimetable(
        "59 12 * * *",
        "0 13 * * *",
        "0 18 * * *",
        "1 18 * * *",
        timezone="Asia/Seoul",
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["misc"],
    on_failure_callback=notify_discord_on_failure,
)
def daily_claude_ping() -> None:
    # queue="ops": ops-vm edge-worker 가 env_file(.env) 로 SSH 접속 정보를 os.environ 에 가짐
    # retries: mac sleep/wake 로 sshd 가 일시 응답하지 않을 수 있음 (daily_meta 와 동일 논리)
    @task(queue="ops", retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=3))
    def send_ping() -> None:
        host = os.environ["CLAUDE_SSH_HOST"]
        user = os.environ["CLAUDE_SSH_USER"]
        key_bytes = base64.b64decode(os.environ["CLAUDE_SSH_KEY_B64"])

        fd, key_path = tempfile.mkstemp(prefix="claude_ssh_key_")
        try:
            os.write(fd, key_bytes)
            os.close(fd)
            os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)

            result = subprocess.run(
                [
                    "ssh",
                    "-i", key_path,
                    "-o", "StrictHostKeyChecking=accept-new",
                    "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=10",
                    f"{user}@{host}",
                    "ping",  # forced command 가 실제 실행 내용을 결정 — 이 인자는 무시됨
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
        finally:
            os.remove(key_path)

        if result.returncode != 0:
            raise RuntimeError(f"ssh rc={result.returncode} stderr={result.stderr[:500]}")
        print(f"[claude] {result.stdout.strip()}")

    send_ping()


daily_claude_ping()

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task


@dag(
    dag_id="lol_list_meta",
    start_date=datetime(2026, 5, 23),
    schedule="0 15 * * *",  # KST 00:00
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["lol-list"],
)
def lol_list_meta() -> None:

    def _inject() -> None:
        os.environ["SUPABASE_URL"] = Variable.get("supabase_url")
        os.environ["SUPABASE_SERVICE_KEY"] = Variable.get("supabase_service_key")

    @task(queue="default")
    def sync_leagues() -> None:
        _inject()
        from collectors.sync_leagues import sync
        sync()

    @task(queue="default")
    def sync_matches_force() -> None:
        _inject()
        from collectors.sync_matches import sync
        sync(force=True)

    @task(queue="default")
    def sync_liquipedia_force() -> None:
        _inject()
        from collectors.sync_liquipedia import sync
        sync(force=True)

    # leagues 먼저, 이후 matches/liquipedia 병렬 force-refresh
    sync_leagues() >> [sync_matches_force(), sync_liquipedia_force()]


lol_list_meta()

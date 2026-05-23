from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task


@dag(
    dag_id="lol_list_matches",
    start_date=datetime(2026, 5, 23),
    schedule="*/10 * * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lol-list"],
)
def lol_list_matches() -> None:

    @task(queue="default")
    def sync() -> None:
        os.environ["SUPABASE_URL"] = Variable.get("supabase_url")
        os.environ["SUPABASE_SERVICE_KEY"] = Variable.get("supabase_service_key")
        from collectors.sync_matches import sync
        sync()

    sync()


lol_list_matches()

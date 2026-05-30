from __future__ import annotations

import os
import platform
import socket
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version

from airflow.sdk import dag, task


@dag(
    dag_id="test_environment",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test_environment() -> None:

    @task(queue="ops")
    def main_node_env() -> None:
        import airflow

        try:
            edge3_ver = version("apache-airflow-providers-edge3")
        except PackageNotFoundError:
            edge3_ver = "not installed"

        print("=== main node (ops-vm) ===")
        print(f"hostname  : {socket.gethostname()}")
        print(f"platform  : {platform.platform()}")
        print(f"python    : {sys.version.split()[0]}")
        print(f"airflow   : {airflow.__version__}")
        print(f"edge3     : {edge3_ver}")
        print(f"executor  : {os.getenv('AIRFLOW__CORE__EXECUTOR', '-')}")
        print(f"api url   : {os.getenv('AIRFLOW__EDGE__API_URL', '-')}")

    @task(queue="default")
    def worker_node_env() -> None:
        import airflow

        try:
            edge3_ver = version("apache-airflow-providers-edge3")
        except PackageNotFoundError:
            edge3_ver = "not installed"

        import shutil
        total, used, free = shutil.disk_usage("/")

        print("=== worker node (worker-vm) ===")
        print(f"hostname  : {socket.gethostname()}")
        print(f"platform  : {platform.platform()}")
        print(f"python    : {sys.version.split()[0]}")
        print(f"airflow   : {airflow.__version__}")
        print(f"edge3     : {edge3_ver}")
        print(f"disk      : total={total >> 30}GB  used={used >> 30}GB  free={free >> 30}GB")
        print(f"api url   : {os.getenv('AIRFLOW__EDGE__API_URL', '-')}")

    @task(queue="gpu")
    def gpu_node_env() -> None:
        import airflow

        try:
            edge3_ver = version("apache-airflow-providers-edge3")
        except PackageNotFoundError:
            edge3_ver = "not installed"

        print("=== gpu node (mac-server / colima VM) ===")
        print(f"hostname  : {socket.gethostname()}")
        print(f"platform  : {platform.platform()}")
        print(f"python    : {sys.version.split()[0]}")
        print(f"airflow   : {airflow.__version__}")
        print(f"edge3     : {edge3_ver}")
        print(f"api url   : {os.getenv('AIRFLOW__EDGE__API_URL', '-')}")

    main_node_env() >> worker_node_env() >> gpu_node_env()


test_environment()

"""
profile_<N>_kubernetes_exec — one Cosmos KUBERNETES_EXEC DAG alone in its file, for bench/profile_exec.py.

Same as scale_<N>_kubernetes_exec (dags/cosmos_scale.py), but:
* the file defines only this DAG, so the per-task "worker startup parse" is what a normal one-DAG file costs;
* dbt runs through /tmp/dbt-timed in the pod (written by bench/profile_exec.py), which prints a line right
  before dbt starts and right after it exits, so the task log shows where the time goes.

Only defined when PROFILE_EXEC_SIZE is set (just profile-exec sets it). Schedule: manual. Nothing downstream.
"""

from __future__ import annotations

# Airflow's safe-mode discovery only parses files containing both "airflow" and "dag"; this comment has both.

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

from demo_ops.settings import CONTAINER, DUCKDB_WRITER_POOL, KUBERNETES_CONN_ID, NAMESPACE, POD_NAME

if size := os.environ.get("PROFILE_EXEC_SIZE"):
    manifest = Path(__file__).parents[1] / "scale" / "target" / f"scale_{size}" / "manifest.json"
    profile_dag = DbtDag(
        dag_id=f"profile_{size}_kubernetes_exec",
        project_config=ProjectConfig(manifest_path=manifest, project_name=f"scale_{size}"),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES_EXEC,
            dbt_project_path=f"/app/scale/scale_{size}",
            dbt_executable_path="/tmp/dbt-timed",
        ),
        operator_args={
            "kubernetes_conn_id": KUBERNETES_CONN_ID,
            "namespace": NAMESPACE,
            "pod_name": POD_NAME,
            "container_name": CONTAINER,
            "pool": DUCKDB_WRITER_POOL,
        },
        start_date=datetime(2026, 11, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=60),
        default_args={"retries": 0},
        tags=["profile", "cosmos"],
        doc_md=__doc__,
    )

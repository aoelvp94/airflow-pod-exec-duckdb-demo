"""
profile_<N>_kubernetes_exec: scale_<N>_kubernetes_exec with dbt run through /tmp/dbt-timed in the pod (written by
bench/profile_exec.py), which prints a line right before dbt starts and right after it exits, so the task log
shows where the time goes. Only defined when PROFILE_EXEC_SIZE is set (`just profile-exec` sets it).
"""

# One DAG per file: airflow re-parses a task's DAG file before running it.
import os

from demo_ops.cosmos_dags import REPO, cosmos_dag

if size := os.environ.get("PROFILE_EXEC_SIZE"):
    dag = cosmos_dag(
        f"profile_{size}_kubernetes_exec", "kubernetes_exec", project_name=f"scale_{size}",
        project_dir=f"/app/scale/scale_{size}", manifest=REPO / "scale" / "target" / f"scale_{size}" / "manifest.json",
        dbt_executable_path="/tmp/dbt-timed", timeout_min=60, tags=["profile"],
    )

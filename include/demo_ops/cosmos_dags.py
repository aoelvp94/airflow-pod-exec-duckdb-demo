"""
Cosmos DbtDags for the comparisons: the same dbt project, one Airflow task per dbt node, three ways.

* KUBERNETES_EXEC     (astronomer-cosmos PR #3027, unreleased): every node is an exec into duckdb/duckdb-dbt-0.
                      No pod per task.
* KUBERNETES          (released): every node is a NEW pod from the same image. The pods mount the forever pod's
                      PVC so models built by one task are visible to the next (fine on single-node kind:
                      ReadWriteOnce is per node).
* WATCHER_KUBERNETES  (released): ONE new pod per DAG run runs the whole `dbt build` (the producer); the per-node
                      tasks are deferrable sensors that read its results. Pod start-up once per run.

Every DAG lives in a file of its own (dags/cosmos_dbt_*.py, and the scale_* stubs `just scale` writes): Airflow
re-parses a task's DAG file before running the task, so a file holding many DbtDags makes every task slower.

Manifests are copied out of the image (`just manifest`, `just scale-manifests`), so parse-time Airflow needs
no dbt install. Tasks use the 1-slot `duckdb_writer` pool (DuckDB is single-writer), except in the watcher DAG:
its sensors wait alongside the producer and must not hold the slot it needs, and the producer is the only
writer there anyway. Schedule: manual (comparison). Nothing downstream.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.sdk import DAG
from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

from demo_ops.pod_args import NEW_POD_ARGS
from demo_ops.settings import CONTAINER, DBT, DUCKDB_WRITER_POOL, KUBERNETES_CONN_ID, NAMESPACE, POD_NAME

REPO = Path(__file__).parents[2]
EXEC_ARGS = {
    "kubernetes_conn_id": KUBERNETES_CONN_ID,
    "namespace": NAMESPACE,
    "pod_name": POD_NAME,
    "container_name": CONTAINER,
    "pool": DUCKDB_WRITER_POOL,
}
MODES = {
    "kubernetes_exec": (ExecutionMode.KUBERNETES_EXEC, EXEC_ARGS, ["pod-exec"]),
    "kubernetes": (
        ExecutionMode.KUBERNETES,
        {**NEW_POD_ARGS, "labels": {"app": "cosmos-kpo"}, "deferrable": False, "pool": DUCKDB_WRITER_POOL},
        ["pod-per-task"],
    ),
    "watcher": (
        ExecutionMode.WATCHER_KUBERNETES,
        # producer: plain KPO, not deferred; consumers: Cosmos's default deferrable sensors
        {**NEW_POD_ARGS, "labels": {"app": "cosmos-watcher"}, "log_events_on_failure": True},
        ["pod-per-run"],
    ),
}
DAG_ARGS = dict(
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},  # comparison: a failure must show up, not be retried away
    doc_md=__doc__,
)


def cosmos_dag(
    dag_id: str,
    mode: str,
    *,
    project_name: str,
    project_dir: str,
    manifest: Path,
    dbt_executable_path: str | None = None,
    timeout_min: int = 30,
    tags: list[str] | None = None,
) -> DbtDag:
    execution_mode, operator_args, mode_tags = MODES[mode]
    execution = {"execution_mode": execution_mode, "dbt_project_path": project_dir}
    if dbt_executable_path:
        execution["dbt_executable_path"] = dbt_executable_path
    return DbtDag(
        dag_id=dag_id,
        project_config=ProjectConfig(manifest_path=manifest, project_name=project_name),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
        execution_config=ExecutionConfig(**execution),
        operator_args=operator_args,
        dagrun_timeout=timedelta(minutes=timeout_min),
        tags=["cosmos", *mode_tags, *(tags or [])],
        **DAG_ARGS,
    )


def demo_dag(mode: str) -> DbtDag:
    """The demo project (dbt/), as cosmos_dbt_<mode>."""
    name = "cosmos_dbt_watcher_kubernetes" if mode == "watcher" else f"cosmos_dbt_{mode}"
    return cosmos_dag(
        name, mode, project_name="duckdb_exec_demo", project_dir="/app/dbt",
        manifest=REPO / "dbt" / "target" / "manifest.json", tags=["dbt", "duckdb"],
    )


def scale_dag(n: int, mode: str):
    """Synthetic project of n models (scale/gen.py), as scale_<n>_<mode>; mode `build` is one `dbt build` exec."""
    project = f"/app/scale/scale_{n}"
    if mode == "build":
        with DAG(dag_id=f"scale_{n}_build", tags=["scale", "pod-exec"], dagrun_timeout=timedelta(minutes=60),
                 **DAG_ARGS) as dag:
            KubernetesPodExecOperator(
                task_id="dbt_build",
                command=[*DBT, "build", "--project-dir", project, "--profiles-dir", "/app/dbt"],
                **EXEC_ARGS,
            )
        return dag
    return cosmos_dag(
        f"scale_{n}_{mode}", mode, project_name=f"scale_{n}", project_dir=project,
        manifest=REPO / "scale" / "target" / f"scale_{n}" / "manifest.json", timeout_min=60, tags=["scale"],
    )

"""
scale_<N>_* — the synthetic projects from scale/gen.py (N models, layers of 10), each run four ways, to see
how every execution mode grows with the number of dbt nodes:

* scale_<N>_build             one `dbt build` exec into the forever pod (KubernetesPodExecOperator), 1 task
* scale_<N>_watcher           Cosmos WATCHER_KUBERNETES: one new pod runs `dbt build`, N sensors follow it
* scale_<N>_kubernetes_exec   Cosmos KUBERNETES_EXEC: one exec into the forever pod per model
* scale_<N>_kubernetes        Cosmos KUBERNETES: one new pod per model

Only generated when SCALE_SIZES is set (`just scale` sets it), so the normal Airflow DAG list stays small.
Needs scale/target/scale_<N>/manifest.json, copied out of the image by `just scale-manifests`.
Writing tasks share the 1-slot duckdb_writer pool (DuckDB is single-writer), as in cosmos_dbt.py.
Schedule: manual (benchmark). Nothing downstream.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.sdk import DAG

from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

from demo_ops.pod_args import NEW_POD_ARGS
from demo_ops.settings import CONTAINER, DBT, DUCKDB_WRITER_POOL, KUBERNETES_CONN_ID, NAMESPACE, POD_NAME

TARGETS = Path(__file__).parents[1] / "scale" / "target"
SIZES = [int(n) for n in os.environ.get("SCALE_SIZES", "").split()]
DAG_ARGS = dict(
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args={"retries": 0},  # benchmark: a failure must show up, not be retried away
    doc_md=__doc__,
)
EXEC_ARGS = {
    "kubernetes_conn_id": KUBERNETES_CONN_ID,
    "namespace": NAMESPACE,
    "pod_name": POD_NAME,
    "container_name": CONTAINER,
    "pool": DUCKDB_WRITER_POOL,
}

for n in SIZES:
    manifest = TARGETS / f"scale_{n}" / "manifest.json"
    if not manifest.exists():
        continue
    project = f"/app/scale/scale_{n}"

    with DAG(dag_id=f"scale_{n}_build", tags=["scale", "pod-exec"], **DAG_ARGS) as build_dag:
        KubernetesPodExecOperator(
            task_id="dbt_build",
            command=[*DBT, "build", "--project-dir", project, "--profiles-dir", "/app/dbt"],
            **EXEC_ARGS,
        )
    globals()[build_dag.dag_id] = build_dag

    common = dict(
        project_config=ProjectConfig(manifest_path=manifest, project_name=f"scale_{n}"),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
        **DAG_ARGS,
    )
    modes = {
        "kubernetes_exec": (ExecutionMode.KUBERNETES_EXEC, EXEC_ARGS),
        "kubernetes": (
            ExecutionMode.KUBERNETES,
            {**NEW_POD_ARGS, "labels": {"app": "scale-kpo"}, "deferrable": False, "pool": DUCKDB_WRITER_POOL},
        ),
        "watcher": (
            ExecutionMode.WATCHER_KUBERNETES,
            {**NEW_POD_ARGS, "labels": {"app": "scale-watcher"}, "log_events_on_failure": True},
        ),
    }
    for suffix, (mode, operator_args) in modes.items():
        dag = DbtDag(
            dag_id=f"scale_{n}_{suffix}",
            execution_config=ExecutionConfig(execution_mode=mode, dbt_project_path=project),
            operator_args=operator_args,
            tags=["scale", "cosmos"],
            **common,
        )
        globals()[dag.dag_id] = dag

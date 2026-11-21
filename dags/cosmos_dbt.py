"""
cosmos_dbt_* — the same dbt project rendered by Cosmos, one Airflow task per dbt node,
executed three ways so they can be compared with the hand-made `duckdb_dbt_exec` DAG:

    raw_customers_seed ─┐
                        ├─> stg_customers.run ─┐
    raw_orders_seed ────┴─> stg_orders.run ─> stg_orders.test ─┴─> customer_revenue.run ─> .test

* cosmos_dbt_kubernetes_exec  ExecutionMode.KUBERNETES_EXEC (astronomer-cosmos PR #3027, unreleased):
                              every node is `kubectl exec` into duckdb/duckdb-dbt-0. No pod per task.
* cosmos_dbt_kubernetes       ExecutionMode.KUBERNETES (released): every node is a NEW pod from the same
                              image. The pods mount the forever pod's PVC so models built by one task are
                              visible to the next (fine on single-node kind: ReadWriteOnce is per node).
* cosmos_dbt_watcher_kubernetes  ExecutionMode.WATCHER_KUBERNETES (released): ONE new pod per DAG run runs
                              the whole `dbt build` (the producer); the per-node tasks are deferrable sensors
                              that read its results. Pod start-up once per run instead of once per node.

All read dbt/target/manifest.json, copied out of the image by `just manifest`, so parse-time Airflow
needs no dbt install. Tasks use the 1-slot `duckdb_writer` pool (DuckDB is single-writer), except in the
watcher DAG: its sensors wait alongside the producer and must not hold the slot it needs, and the
producer is the only writer there anyway.
Schedule: manual (comparison). Nothing downstream.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from kubernetes.client import (
    V1PersistentVolumeClaimVolumeSource,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

from demo_ops.settings import (
    CONTAINER,
    DUCKDB_WRITER_POOL,
    IMAGE,
    KUBERNETES_CONN_ID,
    LAUNCHER_CONN_ID,
    NAMESPACE,
    POD_CPU_REQUEST,
    POD_MEMORY_REQUEST,
    POD_NAME,
)

MANIFEST = Path(__file__).parents[1] / "dbt" / "target" / "manifest.json"
PROJECT_IN_CONTAINER = "/app/dbt"

COMMON = dict(
    project_config=ProjectConfig(manifest_path=MANIFEST, project_name="duckdb_exec_demo"),
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 0},  # comparison: a failure must show up, not be retried away
)

cosmos_dbt_kubernetes_exec = DbtDag(
    dag_id="cosmos_dbt_kubernetes_exec",
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.KUBERNETES_EXEC,
        dbt_project_path=PROJECT_IN_CONTAINER,
    ),
    operator_args={
        "kubernetes_conn_id": KUBERNETES_CONN_ID,
        "namespace": NAMESPACE,
        "pod_name": POD_NAME,
        "container_name": CONTAINER,
        "pool": DUCKDB_WRITER_POOL,
    },
    tags=["cosmos", "dbt", "duckdb", "pod-exec"],
    doc_md=__doc__,
    **COMMON,
)

# What a NEW pod gets, shared by both modes that launch pods: same image, requests and volume as the forever pod.
NEW_POD_ARGS = {
    "kubernetes_conn_id": LAUNCHER_CONN_ID,
    "namespace": NAMESPACE,
    "image": IMAGE,
    "image_pull_policy": "IfNotPresent",
    "container_resources": V1ResourceRequirements(
        requests={"cpu": POD_CPU_REQUEST, "memory": POD_MEMORY_REQUEST},
        limits={"memory": "2Gi"},
    ),
    "node_selector": {"kubernetes.io/os": "linux"},
    "volumes": [
        V1Volume(
            name="data",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name="data-duckdb-dbt-0"),
        )
    ],
    "volume_mounts": [V1VolumeMount(name="data", mount_path="/data")],
    "get_logs": True,
    "on_finish_action": "delete_pod",
}

cosmos_dbt_kubernetes = DbtDag(
    dag_id="cosmos_dbt_kubernetes",
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.KUBERNETES,
        dbt_project_path=PROJECT_IN_CONTAINER,
    ),
    operator_args={
        **NEW_POD_ARGS,
        "labels": {"app": "cosmos-kpo"},
        "deferrable": False,
        "pool": DUCKDB_WRITER_POOL,
    },
    tags=["cosmos", "dbt", "duckdb", "pod-per-task"],
    doc_md=__doc__,
    **COMMON,
)

cosmos_dbt_watcher_kubernetes = DbtDag(
    dag_id="cosmos_dbt_watcher_kubernetes",
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER_KUBERNETES,
        dbt_project_path=PROJECT_IN_CONTAINER,
    ),
    operator_args={
        **NEW_POD_ARGS,
        "labels": {"app": "cosmos-watcher"},
        "log_events_on_failure": True,  # required by the watcher mode, with get_logs
        # producer: plain KPO, not deferred; consumers: Cosmos's default deferrable sensors
    },
    tags=["cosmos", "dbt", "duckdb", "pod-per-run"],
    doc_md=__doc__,
    **COMMON,
)

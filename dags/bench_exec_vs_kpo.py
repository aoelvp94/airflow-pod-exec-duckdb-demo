"""
bench_exec_vs_kpo — same work, two ways: exec into the forever pod vs a fresh pod per task.

    exec_noop_00 ─> … ─> kpo_noop_00 ─> … ─> exec_dbt_00 ─> … ─> kpo_dbt_00 ─> …   (one chain)

* exec_*  KubernetesPodExecOperator into duckdb/duckdb-dbt-0 (warm container, warm dbt parse)
* kpo_*   KubernetesPodOperator, same image + same requests, pod created and deleted per task
* noop    `python -c pass`: pure per-task overhead
* dbt     `dbt build` (2 seeds, 3 models, 5 tests): a small but real dbt step

Tasks run strictly one after another so each duration is the latency of ONE task, not
contention. `bench/report.py` reads the durations back from the Airflow metadata DB.
BENCH_N (env, default 10) = tasks per group. Manual schedule; nothing downstream.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from kubernetes.client import V1ResourceRequirements

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.sdk import DAG, chain

from demo_ops.settings import (
    CONTAINER,
    DBT,
    DBT_DIRS,
    DUCKDB_WRITER_POOL,
    IMAGE,
    KUBERNETES_CONN_ID,
    LAUNCHER_CONN_ID,
    NAMESPACE,
    POD_CPU_REQUEST,
    POD_MEMORY_REQUEST,
    POD_NAME,
)

N = int(os.environ.get("BENCH_N", "10"))

WORKLOADS = {
    "noop": ["python", "-c", "pass"],
    "dbt": [*DBT, "build", *DBT_DIRS],
}


def exec_task(task_id: str, command: list[str]) -> KubernetesPodExecOperator:
    return KubernetesPodExecOperator(
        task_id=task_id,
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        namespace=NAMESPACE,
        pod_name=POD_NAME,
        container_name=CONTAINER,
        command=command,
        pool=DUCKDB_WRITER_POOL,
    )


def kpo_task(task_id: str, command: list[str]) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name="bench-kpo",
        kubernetes_conn_id=LAUNCHER_CONN_ID,
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=command[:1],
        arguments=command[1:],
        container_resources=V1ResourceRequirements(
            requests={"cpu": POD_CPU_REQUEST, "memory": POD_MEMORY_REQUEST},
            limits={"memory": "2Gi"},
        ),
        node_selector={"kubernetes.io/os": "linux"},
        labels={"app": "bench-kpo"},
        get_logs=True,
        on_finish_action="delete_pod",
        deferrable=False,  # deferring would add triggerer round-trips to every measurement
        startup_timeout_seconds=300,
    )


with DAG(
    dag_id="bench_exec_vs_kpo",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["benchmark", "demo", "pod-exec"],
    doc_md=__doc__,
):
    tasks = []
    for workload, command in WORKLOADS.items():
        tasks += [exec_task(f"exec_{workload}_{i:02d}", command) for i in range(N)]
        tasks += [kpo_task(f"kpo_{workload}_{i:02d}", command) for i in range(N)]
    chain(*tasks)

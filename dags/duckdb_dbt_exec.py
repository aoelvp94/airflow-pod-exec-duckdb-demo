"""
duckdb_dbt_exec — run dbt and ad-hoc queries inside ONE long-lived DuckDB pod.

    dbt_seed ──> dbt_build ──> query_customer_revenue ──> report
       (exec)       (exec)            (exec, XCom)          (local)

Instead of starting a fresh pod per task (KubernetesPodOperator / Cosmos KUBERNETES mode),
every task `kubectl exec`s into `duckdb/duckdb-dbt-0`, where dbt + DuckDB are pre-installed
and the warehouse file lives on a PVC. No scheduling, image pull or dbt cold start per task.

Writers share the `duckdb_writer` pool (1 slot) because DuckDB is single-writer.
Schedule: manual (demo). Downstream: anything scheduled on the customer_revenue Asset.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.sdk import DAG, Asset, task

from demo_ops.settings import (
    CONTAINER,
    DBT,
    DBT_DIRS,
    DUCKDB_WRITER_POOL,
    KUBERNETES_CONN_ID,
    NAMESPACE,
    POD_NAME,
)

customer_revenue = Asset("duckdb://duckdb-dbt-0/main/customer_revenue")


def pod_exec(task_id: str, command: list[str], **kwargs) -> KubernetesPodExecOperator:
    return KubernetesPodExecOperator(
        task_id=task_id,
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        namespace=NAMESPACE,
        pod_name=POD_NAME,
        container_name=CONTAINER,
        command=command,
        **kwargs,
    )


with DAG(
    dag_id="duckdb_dbt_exec",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},  # rides out a pod restart
    tags=["dbt", "demo", "duckdb", "pod-exec"],
    doc_md=__doc__,
):
    dbt_seed = pod_exec("dbt_seed", [*DBT, "seed", *DBT_DIRS], pool=DUCKDB_WRITER_POOL)

    dbt_build = pod_exec(
        "dbt_build",
        [*DBT, "build", *DBT_DIRS, "--exclude", "resource_type:seed"],
        pool=DUCKDB_WRITER_POOL,
        outlets=[customer_revenue],
    )

    query_customer_revenue = pod_exec(
        "query_customer_revenue",
        [
            "python",
            "/app/bin/query.py",
            "select name, country, orders, revenue from customer_revenue order by revenue desc",
        ],
        do_xcom_push=True,
    )

    @task
    def report(raw: str) -> None:
        rows = json.loads(raw.strip().splitlines()[-1])
        for row in rows:
            print(f"{row['name']:<10} {row['country']}  orders={row['orders']}  revenue={row['revenue']}")

    dbt_seed >> dbt_build >> query_customer_revenue
    report(query_customer_revenue.output)


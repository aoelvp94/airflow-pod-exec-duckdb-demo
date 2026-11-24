"""cosmos_dbt_kubernetes_exec: the demo dbt project through Cosmos, mode `kubernetes_exec`. See include/demo_ops/cosmos_dags.py."""

# One DAG per file: airflow re-parses a task's DAG file before running it.
from demo_ops.cosmos_dags import demo_dag

dag = demo_dag("kubernetes_exec")

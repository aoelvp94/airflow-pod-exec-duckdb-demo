"""cosmos_dbt_watcher_kubernetes: the demo dbt project through Cosmos, mode `watcher`. See include/demo_ops/cosmos_dags.py."""

# One DAG per file: airflow re-parses a task's DAG file before running it.
from demo_ops.cosmos_dags import demo_dag

dag = demo_dag("watcher")

"""cosmos_dbt_watcher_exec: PROTOTYPE, the watcher with its producer run by exec in the existing pod.
See include/demo_ops/watcher_exec.py."""

# One DAG per file: airflow re-parses a task's DAG file before running it.
from demo_ops.cosmos_dags import demo_dag

dag = demo_dag("watcher_exec")

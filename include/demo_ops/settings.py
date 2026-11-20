"""Shared constants for the demo DAGs."""

KUBERNETES_CONN_ID = "duckdb_k8s"   # supplied as AIRFLOW_CONN_DUCKDB_K8S (see Makefile)
NAMESPACE = "duckdb"
POD_NAME = "duckdb-dbt-0"           # StatefulSet => stable pod name
CONTAINER = "dbt"
DUCKDB_WRITER_POOL = "duckdb_writer"  # 1 slot: DuckDB allows one writer process at a time
DBT = ["dbt", "--no-use-colors"]
DBT_DIRS = ["--project-dir", "/app/dbt", "--profiles-dir", "/app/dbt"]

# Benchmark only
LAUNCHER_CONN_ID = "duckdb_k8s_launcher"  # AIRFLOW_CONN_DUCKDB_K8S_LAUNCHER: may create pods
IMAGE = "duckdb-dbt:dev"
POD_CPU_REQUEST = "250m"   # same requests as the forever pod, so both sides are sized alike
POD_MEMORY_REQUEST = "512Mi"

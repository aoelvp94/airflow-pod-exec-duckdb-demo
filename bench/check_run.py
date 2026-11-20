"""Exit non-zero unless the latest run of DAG_ID succeeded with every task successful.

`airflow dags test` can exit 0 even when tasks fail, so CI checks the metadata DB instead.
"""

import os
import sqlite3
import sys
from pathlib import Path

dag_id = sys.argv[1]
con = sqlite3.connect(Path(os.environ["AIRFLOW_HOME"]) / "airflow.db")
run_id, state = con.execute(
    "select run_id, state from dag_run where dag_id = ? order by id desc limit 1", (dag_id,)
).fetchone()
bad = con.execute(
    "select task_id, state from task_instance where dag_id = ? and run_id = ? and state != 'success'",
    (dag_id, run_id),
).fetchall()
print(f"{dag_id} {run_id}: {state}" + (f", not successful: {bad}" if bad else ""))
sys.exit(0 if state == "success" and not bad else 1)

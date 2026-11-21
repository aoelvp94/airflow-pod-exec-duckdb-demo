"""Run DAGs through a REAL local Airflow (scheduler + triggerer + LocalExecutor), one run after another.

`airflow dags test` runs tasks one at a time in one process, so it cannot run the watcher mode,
whose sensors wait in parallel with the producer. This harness starts `airflow standalone`,
triggers each DAG N times back to back, waits for each run to finish and stops Airflow again.
Durations are then read by bench/compare.py like any other run.

    uv run python bench/scheduled.py --runs 3 duckdb_dbt_exec cosmos_dbt_kubernetes_exec ...
"""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HOME = Path(os.environ["AIRFLOW_HOME"])
DB = HOME / "airflow.db"
TERMINAL = {"success", "failed"}


def airflow(*args: str) -> str:
    return subprocess.run(["airflow", *args], capture_output=True, text=True, check=True).stdout


def latest_run(dag_id: str) -> tuple[str, str] | None:
    with sqlite3.connect(DB) as con:
        return con.execute(
            "select run_id, state from dag_run where dag_id = ? order by id desc limit 1", (dag_id,)
        ).fetchone()


def wait_until_parsed(dag_ids: list[str], timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with sqlite3.connect(DB) as con:
            found = {r[0] for r in con.execute("select dag_id from dag where is_stale = 0")}
        if set(dag_ids) <= found:
            return
        time.sleep(3)
    sys.exit(f"DAGs not parsed in {timeout}s: {set(dag_ids) - found}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dags", nargs="+")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--run-timeout", type=int, default=900)
    a = ap.parse_args()

    log = open(HOME / "standalone.log", "w")
    proc = subprocess.Popen(["airflow", "standalone"], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    failed = []
    try:
        wait_until_parsed(a.dags, timeout=300)
        for dag_id in a.dags:
            airflow("dags", "unpause", dag_id)
            for i in range(a.runs):
                before = latest_run(dag_id)
                airflow("dags", "trigger", dag_id)
                deadline = time.monotonic() + a.run_timeout
                while True:
                    run = latest_run(dag_id)
                    if run and run != before and run[1] in TERMINAL:
                        break
                    if time.monotonic() > deadline:
                        sys.exit(f"{dag_id} run {i + 1} did not finish in {a.run_timeout}s")
                    time.sleep(2)
                print(f"{dag_id} {run[0]}: {run[1]}", flush=True)
                if run[1] != "success":
                    failed.append((dag_id, run[0]))
            airflow("dags", "pause", dag_id)
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

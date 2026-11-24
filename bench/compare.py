"""Compare whole-DAG runs of the ways to run the same dbt project against DuckDB on Kubernetes.

    duckdb_dbt_exec              hand-made: seed + ONE `dbt build` exec (+ query, report)
    cosmos_dbt_kubernetes_exec   Cosmos, one task per dbt node, each an exec into the forever pod
    cosmos_dbt_kubernetes        Cosmos, one task per dbt node, each a NEW pod

Reads the last N successful runs of each from the Airflow DB (runs are in-process `airflow dags test`,
tasks one after another, so wall-clock ≈ sum of task latencies + Airflow's own per-task overhead).
Writes bench/results/compare.json; prints Markdown (also to $GITHUB_STEP_SUMMARY).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
from datetime import datetime
from pathlib import Path

DAGS = {
    "duckdb_dbt_exec": "hand-made exec: 1 `dbt build` exec",
    "cosmos_dbt_kubernetes_exec": "Cosmos KUBERNETES_EXEC: exec per dbt node",
    "cosmos_dbt_kubernetes": "Cosmos KUBERNETES: new pod per dbt node",
    "cosmos_dbt_watcher_kubernetes": "Cosmos WATCHER_KUBERNETES: one new pod per DAG run",
    "cosmos_dbt_watcher_exec": "PROTOTYPE watcher over exec: one dbt build exec per DAG run",
}
OUT = Path(__file__).parent / "results" / "compare.json"


def secs(start: str, end: str) -> float:
    f = lambda s: datetime.fromisoformat(s.replace(" ", "T"))  # noqa: E731
    return (f(end) - f(start)).total_seconds()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--dags", nargs="+", default=list(DAGS), help="DAG ids to read (default: the four above)")
    ap.add_argument("--title", default="Same dbt project, four ways (whole DAG run)")
    ap.add_argument("--since", default="", help="only count runs started at/after this UTC time (YYYY-MM-DD HH:MM:SS)")
    a = ap.parse_args()
    con = sqlite3.connect(Path(os.environ["AIRFLOW_HOME"]) / "airflow.db")

    results = {}
    for dag_id in a.dags:
        runs = con.execute(
            "select run_id from dag_run where dag_id = ? and state = 'success' and start_date >= ? "
            "order by id desc limit ?",
            (dag_id, a.since, a.runs),
        ).fetchall()
        walls, task_sums, n_tasks = [], [], 0
        for (run_id,) in runs:
            tis = con.execute(
                "select start_date, end_date from task_instance where dag_id = ? and run_id = ? "
                "and start_date is not null and end_date is not null",
                (dag_id, run_id),
            ).fetchall()
            n_tasks = len(tis)
            starts, ends = [t[0] for t in tis], [t[1] for t in tis]
            walls.append(secs(min(starts), max(ends)))  # first task start -> last task end
            task_sums.append(sum(secs(s, e) for s, e in tis))
        if walls:
            results[dag_id] = {
                "runs": len(walls),
                "tasks": n_tasks,
                "wall_p50_s": round(statistics.median(walls), 2),
                "wall_min_s": round(min(walls), 2),
                "wall_max_s": round(max(walls), 2),
                "task_time_p50_s": round(statistics.median(task_sums), 2),
            }

    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(results, indent=2))

    lines = [
        f"### {a.title}",
        "",
        "| DAG | how | tasks | runs | wall p50 s | min–max s | sum of task time p50 s |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for dag_id in a.dags:
        how = DAGS.get(dag_id, "")
        r = results.get(dag_id)
        if r:
            lines.append(
                f"| `{dag_id}` | {how} | {r['tasks']} | {r['runs']} | {r['wall_p50_s']} | "
                f"{r['wall_min_s']}–{r['wall_max_s']} | {r['task_time_p50_s']} |"
            )
        else:
            lines.append(f"| `{dag_id}` | {how} | – | 0 | – | – | – |")
    md = "\n".join(lines) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

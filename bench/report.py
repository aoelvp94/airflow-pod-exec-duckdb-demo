"""Summarise the latest bench_exec_vs_kpo run from the Airflow metadata DB.

Writes bench/results/latency.json and prints a Markdown table (also appended to
$GITHUB_STEP_SUMMARY when running in GitHub Actions).
"""

from __future__ import annotations

import json
import os
import sqlite3
import statistics
import sys
from datetime import datetime
from pathlib import Path

DAG_ID = "bench_exec_vs_kpo"
OUT = Path(__file__).parent / "results" / "latency.json"


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    k = (len(ordered) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace(" ", "T"))


def main() -> int:
    db = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
    con = sqlite3.connect(db)
    run = con.execute(
        "select run_id, state from dag_run where dag_id = ? order by id desc limit 1", (DAG_ID,)
    ).fetchone()
    if not run:
        print(f"no {DAG_ID} run found", file=sys.stderr)
        return 1
    run_id, run_state = run
    rows = con.execute(
        "select task_id, state, start_date, end_date from task_instance where dag_id = ? and run_id = ?",
        (DAG_ID, run_id),
    ).fetchall()

    groups: dict[str, list[float]] = {}
    failed = [task_id for task_id, state, *_ in rows if state != "success"]
    for task_id, state, start, end in rows:
        if state == "success" and start and end:
            group = task_id.rsplit("_", 1)[0]  # exec_noop_03 -> exec_noop
            groups.setdefault(group, []).append((parse(end) - parse(start)).total_seconds())

    stats = {
        g: {
            "n": len(v),
            "mean_s": round(statistics.mean(v), 3),
            "p50_s": round(pct(v, 0.50), 3),
            "p95_s": round(pct(v, 0.95), 3),
            "min_s": round(min(v), 3),
            "max_s": round(max(v), 3),
        }
        for g, v in sorted(groups.items())
    }
    for workload in ("noop", "dbt"):
        e, k = stats.get(f"exec_{workload}"), stats.get(f"kpo_{workload}")
        if e and k:
            stats[f"overhead_{workload}"] = {
                "kpo_minus_exec_p50_s": round(k["p50_s"] - e["p50_s"], 3),
                "speedup_p50": round(k["p50_s"] / e["p50_s"], 1),
            }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"run_id": run_id, "run_state": run_state, "failed": failed, "stats": stats}, indent=2))

    lines = [
        f"### Per-task latency: exec vs pod-per-task (run `{run_id}`, {run_state})",
        "",
        "| group | n | p50 s | p95 s | min s | max s |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for g, s in stats.items():
        if "n" in s:
            lines.append(f"| {g} | {s['n']} | {s['p50_s']} | {s['p95_s']} | {s['min_s']} | {s['max_s']} |")
    lines.append("")
    for workload in ("noop", "dbt"):
        o = stats.get(f"overhead_{workload}")
        if o:
            lines.append(
                f"- **{workload}**: a fresh pod adds **{o['kpo_minus_exec_p50_s']} s** per task "
                f"(p50), {o['speedup_p50']}× slower than exec"
            )
    if failed:
        lines.append(f"- ⚠️ failed tasks: {', '.join(sorted(failed))}")
    md = "\n".join(lines) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)
    return 0 if run_state == "success" and not failed else 1


if __name__ == "__main__":
    sys.exit(main())

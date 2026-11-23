"""Where does a Cosmos KUBERNETES_EXEC task spend its time? Breaks every task of the latest successful run of a
DAG into segments, from its task log (timestamps of known lines) and the Airflow DB (queued/start/end).

    install   writes /tmp/dbt-timed into the pod: prints a line right before dbt starts and right after it exits
    report    reads the latest successful run of --dag and prints the median of each segment over its tasks

    just profile-exec 50   # install, run profile_50_kubernetes_exec once through a real scheduler, report

All timestamps except queued/start/end are when the Airflow task logged the line, i.e. one clock (the worker's).
Writes bench/results/profile-exec.json and prints Markdown (also to $GITHUB_STEP_SUMMARY).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import subprocess
from datetime import datetime
from pathlib import Path

HOME = Path(os.environ["AIRFLOW_HOME"])
OUT = Path(__file__).parent / "results" / "profile-exec.json"
CONTEXT = os.environ.get("KIND_CONTEXT", "kind-duckdb-exec")
WRAPPER = """#!/bin/sh
echo "PROFILE start"
dbt "$@"
rc=$?
echo "PROFILE end rc=$rc"
exit $rc
"""

# (segment name, milestone it starts at, milestone it ends at). Milestones: substrings of task log events.
MILESTONES = {
    "log_start": None,  # first line of the task log
    "parsed": "Worker startup parse complete",
    "exec": "Executing command in container",
    "wrapper_start": "[stdout] PROFILE start",
    "dbt_running": "Running with dbt=",
    "found": "Found ",
    "first_node": " START ",
    "finished": "Finished running",
    "wrapper_end": "[stdout] PROFILE end",
    "post": "::group::Post Execute",
    "log_end": None,  # last line of the task log
}
SEGMENTS = [
    ("task start → first log line", "ti_start", "log_start"),
    ("worker re-parses the DAG file", "log_start", "parsed"),
    ("operator setup, read the pod", "parsed", "exec"),
    ("exec connection + process spawn", "exec", "wrapper_start"),
    ("dbt CLI start-up (imports)", "wrapper_start", "dbt_running"),
    ("adapter + load partial parse", "dbt_running", "found"),
    ("connection + run setup", "found", "first_node"),
    ("the model itself", "first_node", "finished"),
    ("dbt exit (writes artifacts)", "finished", "wrapper_end"),
    ("exec read loop notices the end", "wrapper_end", "post"),
    ("post-execute (until the task's end time)", "post", "ti_end"),
]


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T")).replace(tzinfo=None)


def install() -> None:
    subprocess.run(
        ["kubectl", "--context", CONTEXT, "-n", "duckdb", "exec", "-i", "duckdb-dbt-0", "-c", "dbt", "--",
         "sh", "-c", "cat > /tmp/dbt-timed && chmod +x /tmp/dbt-timed"],
        input=WRAPPER, text=True, check=True,
    )
    print("installed /tmp/dbt-timed in the pod")


def task_milestones(log: Path) -> dict[str, datetime]:
    lines = [json.loads(line) for line in log.read_text().splitlines() if line.strip().startswith("{")]
    found: dict[str, datetime] = {"log_start": ts(lines[0]["timestamp"]), "log_end": ts(lines[-1]["timestamp"])}
    for line in lines:
        for name, needle in MILESTONES.items():
            if needle and name not in found and needle in str(line.get("event", "")):
                found[name] = ts(line["timestamp"])
    return found


def report(dag_id: str, out: Path) -> None:
    con = sqlite3.connect(HOME / "airflow.db")
    run_id, run_start = con.execute(
        "select run_id, start_date from dag_run where dag_id = ? and state = 'success' order by id desc limit 1", (dag_id,)
    ).fetchone()
    tis = con.execute(
        "select task_id, queued_dttm, start_date, end_date from task_instance where dag_id = ? and run_id = ?",
        (dag_id, run_id),
    ).fetchall()
    per_segment: dict[str, list[float]] = {name: [] for name, _, _ in SEGMENTS}
    durations, queued_waits = [], []
    for task_id, queued, start, end in tis:
        log = HOME / "logs" / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / "attempt=1.log"
        m = task_milestones(log)
        m["ti_start"], m["ti_end"] = ts(start), ts(end)
        durations.append((ts(end) - ts(start)).total_seconds())
        if queued:
            queued_waits.append((ts(start) - ts(queued)).total_seconds())
        for name, a, b in SEGMENTS:
            if a in m and b in m:
                per_segment[name].append((m[b] - m[a]).total_seconds())
    starts, ends = [ts(t[2]) for t in tis], [ts(t[3]) for t in tis]
    wall = (max(ends) - min(starts)).total_seconds()
    between = (wall - sum(durations)) / max(len(tis) - 1, 1)

    med = lambda xs: round(statistics.median(xs), 2) if xs else None  # noqa: E731
    rows = [(name, med(v), len(v)) for name, v in per_segment.items()]
    res = {
        "dag_id": dag_id, "run_id": run_id, "tasks": len(tis), "wall_s": round(wall, 1),
        "task_median_s": med(durations), "between_tasks_s": round(between, 2),
        "queued_to_start_median_s": med(queued_waits),
        "segments_median_s": {name: v for name, v, _ in rows},
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=2))

    lines = [
        f"### Where a KUBERNETES_EXEC task spends its time (`{dag_id}`, {len(tis)} tasks, median)",
        "",
        "| segment | median s |",
        "|---|---:|",
        *[f"| {name} | {v if v is not None else '–'} |" for name, v, _ in rows],
        f"| **task, start → end** | **{res['task_median_s']}** |",
        f"| between tasks (wall − sum of tasks, per gap) | {res['between_tasks_s']} |",
        f"| of which queued → start | {res['queued_to_start_median_s']} |",
        "",
        f"Whole run: {res['wall_s']} s.",
    ]
    md = "\n".join(lines) + "\n"
    print(md)
    if gh := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(gh, "a") as f:
            f.write(md)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["install", "report"])
    ap.add_argument("--dag", default="profile_50_kubernetes_exec")
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()
    install() if a.action == "install" else report(a.dag, a.out)


if __name__ == "__main__":
    main()

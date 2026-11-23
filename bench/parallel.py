"""Concurrent dbt processes in the same pod: shared project directory vs one workspace each.

The PR's docs say parallel execution needs independent workspaces or pods, "including separate dbt target and
log paths". This runs P concurrent `dbt build`s of the demo project in the forever pod, --rounds times, two ways:

    shared     every process uses /app/dbt's target/ and logs/ (what you get by just dropping the pool)
    separate   each process gets its own DBT_TARGET_PATH and DBT_LOG_PATH under /tmp/ws/<i>

DuckDB allows one writer per file, so every process writes its own /data/par_<i>.duckdb in both layouts; that's
what running DuckDB in parallel means anyway. Commands have the same shape Cosmos KUBERNETES_EXEC sends
(`env -- VAR=... dbt build --project-dir ...`), sent through `kubectl exec` (the same exec API), so the full
output and exit code of failures are kept. Records per process: exit code, duration, whether it did a full
parse, and error lines. After the shared rounds, one sequential `dbt parse` checks the shared target/ is usable.
Cleans up the extra DuckDB files and workspaces and restores /app/dbt's partial-parse state at the end.

    just parallel                 # P=4, 3 rounds per layout
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

OUT = Path(__file__).parent / "results" / "parallel.json"
CONTEXT = os.environ.get("KIND_CONTEXT", "kind-duckdb-exec")
POD = ["kubectl", "--context", CONTEXT, "-n", "duckdb", "exec", "duckdb-dbt-0", "-c", "dbt", "--"]
FULL_PARSE = re.compile(r"Unable to do partial parsing|Partial parse save file not found|full re-parse", re.I)
ERROR = re.compile(r"(error|exception|traceback|corrupt|msgpack)", re.I)


def run_one(layout: str, i: int) -> dict:
    env = [f"DUCKDB_PATH=/data/par_{i}.duckdb"]
    if layout == "separate":
        env += [f"DBT_TARGET_PATH=/tmp/ws/{i}/target", f"DBT_LOG_PATH=/tmp/ws/{i}/logs"]
    cmd = [*POD, "env", "--", *env, "dbt", "--no-use-colors", "build", "--project-dir", "/app/dbt"]
    t = time.monotonic()
    r = subprocess.run(cmd, capture_output=True, text=True)
    out = r.stdout + r.stderr
    errors = sorted({line.strip()[:200] for line in out.splitlines() if ERROR.search(line) and "ERROR=0" not in line
                     and "Completed successfully" not in line})
    return {
        "layout": layout,
        "proc": i,
        "rc": r.returncode,
        "s": round(time.monotonic() - t, 2),
        "full_parse": bool(FULL_PARSE.search(out)),
        "errors": errors[:5],
    }


def pod_sh(script: str) -> subprocess.CompletedProcess:
    return subprocess.run([*POD, "bash", "-c", script], capture_output=True, text=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()

    pod_sh("rm -rf /tmp/ws /data/par_*.duckdb*")
    runs: list[dict] = []
    checks: dict[str, str] = {}
    try:
        for layout in ("shared", "separate"):
            for rnd in range(a.rounds):
                with ThreadPoolExecutor(a.procs) as pool:
                    batch = list(pool.map(lambda i: run_one(layout, i), range(a.procs)))
                for b in batch:
                    b["round"] = rnd
                runs += batch
                print(f"{layout} round {rnd + 1}: rc={[b['rc'] for b in batch]} full_parse={[b['full_parse'] for b in batch]}"
                      f" s={[b['s'] for b in batch]}", flush=True)
                for b in batch:
                    for e in b["errors"]:
                        print(f"   proc {b['proc']}: {e}", flush=True)
            if layout == "shared":
                r = pod_sh("cd /app/dbt && dbt --no-use-colors parse 2>&1 | tail -3")
                checks["shared_target_parse_after"] = f"rc={r.returncode}: {r.stdout.strip()[-200:]}"
                print(f"sequential parse of the shared project afterwards: {checks['shared_target_parse_after']}", flush=True)
    finally:
        pod_sh("rm -rf /tmp/ws /data/par_*.duckdb*; cd /app/dbt && dbt --quiet parse")

    summary = {}
    for layout in ("shared", "separate"):
        rs = [r for r in runs if r["layout"] == layout]
        later = [r for r in rs if r["round"] > 0]  # round 1 always full-parses: new DUCKDB_PATH / new target path
        summary[layout] = {
            "processes": len(rs),
            "failed": sum(r["rc"] != 0 for r in rs),
            "full_parses_after_round_1": sum(r["full_parse"] for r in later),
            "runs_after_round_1": len(later),
            "median_s": round(statistics.median(r["s"] for r in rs), 2),
            "error_samples": sorted({e for r in rs for e in r["errors"]})[:5],
        }
    res = {"procs": a.procs, "rounds": a.rounds, "summary": summary, "checks": checks, "runs": runs}
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(res, indent=2))

    lines = [
        f"### {a.procs} concurrent `dbt build`s in one pod, {a.rounds} rounds",
        "",
        "| layout | failed | full parses after round 1 | median time |",
        "|---|---:|---:|---:|",
    ]
    for layout, s in summary.items():
        lines.append(f"| {layout} target/logs | {s['failed']} of {s['processes']} | "
                     f"{s['full_parses_after_round_1']} of {s['runs_after_round_1']} | {s['median_s']} s |")
    md = "\n".join(lines) + "\n"
    print(md)
    if gh := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(gh, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

"""How long does dbt take to get ready inside the forever pod, by project size?

For each synthetic project (scale/gen.py, baked into the image and pre-parsed), times inside the pod:
    startup   `dbt --version`: interpreter + dbt imports, no project
    partial   `dbt parse` with the pre-parsed state (what every exec pays when nothing changed)
    full      `dbt parse --no-partial-parse` (what a changed profile, env var or project costs)
Median of --repeat runs each; afterwards one normal `dbt parse` restores the partial-parse state.
Writes bench/results/scale-parse.json.

    uv run python bench/scale_parse.py 10 50 200
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

OUT = Path(__file__).parent / "results" / "scale-parse.json"
CONTEXT = os.environ.get("KIND_CONTEXT", "kind-duckdb-exec")

IN_POD = r"""
import json, os, statistics, subprocess, sys, time
project, repeat = sys.argv[1], int(sys.argv[2])
def t(cmd):
    runs = []
    for _ in range(repeat):
        s = time.monotonic()
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        runs.append(time.monotonic() - s)
    return round(statistics.median(runs), 2)
res = {
    "startup_s": t(["dbt", "--version"]),
    # --project-dir: DBT_PROJECT_DIR in the image wins over the working directory
    "partial_parse_s": t(["dbt", "--quiet", "parse", "--project-dir", project]),
    "full_parse_s": t(["dbt", "--quiet", "parse", "--no-partial-parse", "--project-dir", project]),
}
subprocess.run(["dbt", "--quiet", "parse", "--project-dir", project], check=True, stdout=subprocess.DEVNULL)  # restore partial-parse state
print(json.dumps(res))
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sizes", nargs="+", type=int)
    ap.add_argument("--repeat", type=int, default=3)
    a = ap.parse_args()

    results = {}
    for n in a.sizes:
        out = subprocess.run(
            ["kubectl", "--context", CONTEXT, "-n", "duckdb", "exec", "duckdb-dbt-0", "-c", "dbt", "--",
             "python", "-c", IN_POD, f"/app/scale/scale_{n}", str(a.repeat)],
            capture_output=True, text=True, check=True,
        ).stdout
        results[n] = json.loads(out.strip().splitlines()[-1])
        print(f"{n} models: {results[n]}", flush=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()

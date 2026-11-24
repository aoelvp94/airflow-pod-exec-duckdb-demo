"""Join `just scale` results into one table: whole-run time of each mode by project size, plus dbt's parse costs.

Reads bench/results/compare-scale.json (bench/compare.py) and bench/results/scale-parse.json (bench/scale_parse.py),
writes bench/results/scale.json and prints Markdown (also to $GITHUB_STEP_SUMMARY). "per model" is the slope
between the smallest and largest size measured, i.e. what one more model adds to a run.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

RESULTS = Path(__file__).parent / "results"
MODES = [
    ("build", "one `dbt build` exec"),
    ("watcher", "Cosmos WATCHER_KUBERNETES"),
    ("watcher_exec", "watcher over exec (prototype)"),
    ("kubernetes_exec", "Cosmos KUBERNETES_EXEC"),
    ("kubernetes", "Cosmos KUBERNETES"),
]


def main() -> None:
    compare = json.loads((RESULTS / "compare-scale.json").read_text())
    parse_path = RESULTS / "scale-parse.json"
    parse = json.loads(parse_path.read_text()) if parse_path.exists() else {}
    sizes = sorted({int(d.split("_")[1]) for d in compare})

    table = {m: {n: compare.get(f"scale_{n}_{m}", {}).get("wall_p50_s") for n in sizes} for m, _ in MODES}
    out = {"sizes": sizes, "wall_p50_s": table, "parse": parse}
    (RESULTS / "scale.json").write_text(json.dumps(out, indent=2))

    cell = lambda v: f"{v:.1f} s" if v is not None else "–"  # noqa: E731
    head = " | ".join(f"{n} models" for n in sizes)
    lines = [
        "### Whole run by project size (models)",
        "",
        f"| mode | {head} | per model |",
        "|---|" + "---:|" * (len(sizes) + 1),
    ]
    for mode, label in MODES:
        row = table[mode]
        have = [n for n in sizes if row[n] is not None]
        slope = (
            f"{(row[have[-1]] - row[have[0]]) / (have[-1] - have[0]):.2f} s"
            if len(have) >= 2 else "–"
        )
        lines.append(f"| {label} | " + " | ".join(cell(row[n]) for n in sizes) + f" | {slope} |")
    if parse:
        lines += [
            "",
            "### dbt inside the forever pod, by project size (median)",
            "",
            "| models | `dbt --version` | `dbt parse` (partial) | `dbt parse --no-partial-parse` (full) |",
            "|---:|---:|---:|---:|",
        ]
        for n, r in sorted(parse.items(), key=lambda kv: int(kv[0])):
            lines.append(f"| {n} | {r['startup_s']} s | {r['partial_parse_s']} s | {r['full_parse_s']} s |")
    md = "\n".join(lines) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

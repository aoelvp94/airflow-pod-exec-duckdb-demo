"""Render shareable PNG charts from benchmark results (bench/report.py and bench/compare.py output).

    uv run --with matplotlib python bench/plot.py \
        --latency bench/results/ci-44754491402/latency.json \
        --compare bench/results/ci-44786440548/compare.json \
        --out docs/img

Static images for GitHub comments and slides: every bar carries its value, and the headline is
computed from the data. Colors: blue = exec into the running pod, orange = a new pod.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SURFACE, TEXT, TEXT_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e0"
EXEC, POD = "#2a78d6", "#eb6834"  # validated pair (dataviz reference palette, slots 1-2)

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": TEXT,
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT_2, "xtick.color": TEXT_2, "ytick.color": TEXT,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
})


def fmt(v: float) -> str:
    return f"{v:.1f} s" if v >= 10 else f"{v:.2f} s".replace(".00 s", " s")


def frame(ax, xmax: float) -> None:
    ax.set_xlim(0, xmax)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", length=0)
    ax.xaxis.set_major_formatter(lambda x, _: f"{x:g} s")


def headline(fig, title: str, subtitle: str) -> None:
    fig.text(0.02, 0.95, title, fontsize=14, fontweight="bold", va="top")
    fig.text(0.02, 0.87, subtitle, fontsize=9.5, color=TEXT_2, va="top")


def legend(fig) -> None:
    from matplotlib.patches import Patch

    fig.legend(handles=[Patch(color=EXEC, label="exec into an existing pod (Airflow does not manage it)"),
                        Patch(color=POD, label="Airflow creates and deletes the pod")],
               loc="lower left", bbox_to_anchor=(0.02, 0.0), ncol=2, frameon=False, fontsize=9, labelcolor=TEXT_2)


def per_task(latency: dict, out: Path, source: str) -> Path:
    s = latency["stats"]
    groups = [("Empty command", "noop"), ("dbt build", "dbt")]
    fig, ax = plt.subplots(figsize=(8, 3.6), dpi=200)
    fig.subplots_adjust(left=0.2, right=0.93, top=0.74, bottom=0.2)
    y, ticks, labels = 0.0, [], []
    for label, key in groups:
        e, p = s[f"exec_{key}"]["p50_s"], s[f"kpo_{key}"]["p50_s"]
        for v, c, dy in ((e, EXEC, 0.0), (p, POD, 0.42)):
            ax.barh(y + dy, v, height=0.36, color=c, edgecolor=SURFACE, linewidth=2)
            ax.text(v + 0.08, y + dy, fmt(v), va="center", fontsize=9.5, color=TEXT)
        ticks.append(y + 0.21)
        labels.append(label)
        y += 1.25
    ax.set_yticks(ticks, labels)
    ax.invert_yaxis()
    xmax = max(s["kpo_noop"]["p50_s"], s["kpo_dbt"]["p50_s"]) * 1.18
    frame(ax, xmax)
    added = s["kpo_noop"]["p50_s"] - s["exec_noop"]["p50_s"]
    n = s["exec_noop"]["n"]
    headline(fig, f"A new pod adds {added:.1f} s to every task",
             f"Median seconds per Airflow task, {n} tasks per bar · {source}")
    legend(fig)
    path = out / "per_task.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def three_ways(compare: dict, out: Path, source: str) -> Path:
    rows = [
        ("One dbt build exec\n(hand-made, 4 tasks)", "duckdb_dbt_exec", EXEC),
        ("Cosmos KUBERNETES_EXEC\n(exec per dbt node, 8 tasks)", "cosmos_dbt_kubernetes_exec", EXEC),
        ("Cosmos KUBERNETES\n(new pod per dbt node, 8 tasks)", "cosmos_dbt_kubernetes", POD),
        ("Cosmos WATCHER_KUBERNETES\n(1 new pod per run, 1 dbt build)", "cosmos_dbt_watcher_kubernetes", POD),
    ]
    rows = [r for r in rows if r[1] in compare]  # the watcher only exists in scheduled comparisons
    fig, ax = plt.subplots(figsize=(8, 2.6 + 0.6 * len(rows)), dpi=200)
    fig.subplots_adjust(left=0.33, right=0.93, top=1 - 0.95 / (2.6 + 0.6 * len(rows)), bottom=0.7 / (2.6 + 0.6 * len(rows)))
    for i, (label, dag, color) in enumerate(rows):
        r = compare[dag]
        ax.barh(i, r["wall_p50_s"], height=0.55, color=color, edgecolor=SURFACE, linewidth=2)
        ax.errorbar(r["wall_p50_s"], i, xerr=[[r["wall_p50_s"] - r["wall_min_s"]], [r["wall_max_s"] - r["wall_p50_s"]]],
                    fmt="none", ecolor=TEXT, elinewidth=1.2, capsize=4)
        ax.text(r["wall_max_s"] + 1.0, i, fmt(r["wall_p50_s"]), va="center", fontsize=9.5)
    ax.set_yticks(range(len(rows)), [r[0] for r in rows], fontsize=9.5)
    ax.invert_yaxis()
    frame(ax, max(compare[d]["wall_max_s"] for _, d, _ in rows) * 1.16)
    speedup = compare["cosmos_dbt_kubernetes"]["wall_p50_s"] / compare["cosmos_dbt_kubernetes_exec"]["wall_p50_s"]
    runs = compare["cosmos_dbt_kubernetes_exec"]["runs"]
    title = (
        "Same dbt project, four ways, through a real scheduler"
        if len(rows) == 4
        else f"Same dbt project: exec per node runs {speedup:.1f}× faster than a pod per node"
    )
    headline(fig, title,
             f"Whole DAG run, median of {runs} (whiskers: fastest–slowest) · {source}")
    legend(fig)
    path = out / ("cosmos_four_ways.png" if len(rows) == 4 else "cosmos_three_ways.png")
    fig.savefig(path)
    plt.close(fig)
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency", type=Path, required=True)
    ap.add_argument("--compare", type=Path, required=True)
    ap.add_argument("--source", default="GitHub Actions ubuntu-latest, single-node kind, image cached")
    ap.add_argument("--compare-source", default=None, help="subtitle for the comparison chart, if different")
    ap.add_argument("--out", type=Path, default=Path("docs/img"))
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    print(per_task(json.loads(a.latency.read_text()), a.out, a.source))
    print(three_ways(json.loads(a.compare.read_text()), a.out, a.compare_source or a.source))


if __name__ == "__main__":
    main()

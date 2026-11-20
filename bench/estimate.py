"""Back-of-envelope model: exec into a forever pod vs a fresh pod per task, at scale.

Takes the per-task overheads MEASURED by bench/report.py (bench/results/latency.json) and adds
cloud-only costs that kind cannot show (image pulls, node provisioning) as explicit ASSUMPTIONS.

    uv run python bench/estimate.py                       # defaults + measured overheads
    uv run python bench/estimate.py --work-s 45 --node-cold-rate 0.3 --tasks 500 5000

Every input is printed with its source (measured / assumed) so the result can be argued with.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

LATENCY = Path(__file__).parent / "results" / "latency.json"

# AWS Fargate Linux/x86 on-demand list prices, us-east-1. Override for your cloud / instance type.
DEFAULT_PRICE_VCPU_H = 0.04048
DEFAULT_PRICE_GB_H = 0.004445


def measured() -> dict:
    if not LATENCY.exists():
        return {}
    s = json.loads(LATENCY.read_text())["stats"]
    out = {}
    if "exec_noop" in s and "kpo_noop" in s:
        out["exec_overhead_s"] = s["exec_noop"]["p50_s"]
        out["pod_overhead_s"] = s["kpo_noop"]["p50_s"]
    if "exec_dbt" in s and "exec_noop" in s:
        out["work_s"] = round(s["exec_dbt"]["p50_s"] - s["exec_noop"]["p50_s"], 3)
    return out


def main() -> None:
    m = measured()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tasks", type=int, nargs="+", default=[100, 1_000, 5_000, 20_000], help="tasks per day")
    ap.add_argument("--work-s", type=float, default=m.get("work_s", 20.0), help="real work per task")
    ap.add_argument("--exec-overhead-s", type=float, default=m.get("exec_overhead_s", 1.0))
    ap.add_argument("--pod-overhead-s", type=float, default=m.get("pod_overhead_s", 8.0))
    ap.add_argument("--image-pull-s", type=float, default=20.0, help="cold image pull (assumed)")
    ap.add_argument("--pull-miss-rate", type=float, default=0.05, help="share of pods landing on a node without the image")
    ap.add_argument("--node-provision-s", type=float, default=60.0, help="Karpenter/cluster-autoscaler new node (assumed)")
    ap.add_argument("--node-cold-rate", type=float, default=0.10, help="share of pods that must wait for a new node")
    ap.add_argument("--pod-cpu", type=float, default=0.25, help="vCPU requested by each on-demand pod")
    ap.add_argument("--pod-mem-gb", type=float, default=0.5)
    ap.add_argument("--forever-cpu", type=float, default=1.0, help="vCPU reserved 24/7 by the forever pod")
    ap.add_argument("--forever-mem-gb", type=float, default=2.0)
    ap.add_argument("--price-vcpu-h", type=float, default=DEFAULT_PRICE_VCPU_H)
    ap.add_argument("--price-gb-h", type=float, default=DEFAULT_PRICE_GB_H)
    ap.add_argument("--dag-depth", type=int, default=10, help="tasks on a DAG's critical path")
    a = ap.parse_args()

    def src(key: str) -> str:
        return "measured on kind" if key in m and getattr(a, key) == m[key] else "assumed"

    exec_task_s = a.exec_overhead_s + a.work_s
    cloud_extra_s = a.pull_miss_rate * a.image_pull_s + a.node_cold_rate * a.node_provision_s
    pod_task_s = a.pod_overhead_s + cloud_extra_s + a.work_s
    delta_s = pod_task_s - exec_task_s

    pod_rate_h = a.pod_cpu * a.price_vcpu_h + a.pod_mem_gb * a.price_gb_h
    # pod is billed while it exists: start-up + pull + work (node wait happens before the pod runs)
    pod_life_s = a.pod_overhead_s + a.pull_miss_rate * a.image_pull_s + a.work_s
    pod_cost_per_task = pod_life_s / 3600 * pod_rate_h
    forever_cost_day = 24 * (a.forever_cpu * a.price_vcpu_h + a.forever_mem_gb * a.price_gb_h)
    break_even = forever_cost_day / pod_cost_per_task if pod_cost_per_task else float("inf")
    writer_capacity = 86_400 / exec_task_s

    lines = [
        "### Estimate: forever pod (exec) vs pod per task",
        "",
        "**Inputs**",
        "",
        f"- work per task: {a.work_s:.2f} s ({src('work_s')})",
        f"- exec overhead: {a.exec_overhead_s:.2f} s ({src('exec_overhead_s')})",
        f"- pod-per-task overhead: {a.pod_overhead_s:.2f} s ({src('pod_overhead_s')}; warm node, image cached)",
        f"- cloud extras per pod: {a.pull_miss_rate:.0%} × {a.image_pull_s:.0f} s pull + "
        f"{a.node_cold_rate:.0%} × {a.node_provision_s:.0f} s new node = {cloud_extra_s:.1f} s avg (assumed)",
        f"- prices: ${a.price_vcpu_h}/vCPU-h, ${a.price_gb_h}/GB-h; on-demand pod {a.pod_cpu} vCPU / {a.pod_mem_gb} GB, "
        f"forever pod {a.forever_cpu} vCPU / {a.forever_mem_gb} GB",
        "",
        "**Per task**",
        "",
        f"- latency: exec **{exec_task_s:.1f} s** vs pod **{pod_task_s:.1f} s** → {delta_s:.1f} s saved "
        f"({pod_task_s / exec_task_s:.1f}×)",
        f"- DAG with {a.dag_depth} sequential tasks: **{a.dag_depth * delta_s / 60:.1f} min** shorter per run",
        f"- forever pod: ${forever_cost_day:.2f}/day fixed; pod-per-task: ${pod_cost_per_task * 1000:.3f} per 1k tasks "
        f"→ cost break-even at **{break_even:,.0f} tasks/day**",
        f"- single-writer ceiling: one forever pod can run at most **{writer_capacity:,.0f} writing tasks/day** "
        "(DuckDB serialises writers)",
        "",
        "| tasks/day | wait removed (task-h/day) | pod-per-task $/day | forever pod $/day | forever pod busy |",
        "|---:|---:|---:|---:|---:|",
    ]
    for t in a.tasks:
        busy = t * exec_task_s / 86_400
        flag = " ⚠️ over capacity" if busy > 1 else ""
        lines.append(
            f"| {t:,} | {t * delta_s / 3600:,.1f} | {t * pod_cost_per_task:,.2f} | {forever_cost_day:,.2f} | "
            f"{busy:.0%}{flag} |"
        )
    lines += [
        "",
        "Not modelled: Airflow's own per-task cost (same on both sides; under KubernetesExecutor every task "
        "already gets a worker pod, so exec removes the *second* pod only), API-server/etcd load from pod "
        "churn, pod IP exhaustion, and the risk side (shared blast radius, one writer at a time).",
    ]
    md = "\n".join(lines) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

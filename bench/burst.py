"""Burst test: create K pods at once (what a big fan-out of KubernetesPodOperator tasks does)
and measure how long each takes to be scheduled, start and finish.

    uv run python bench/burst.py --pods 50

Uses the airflow-launcher kubeconfig. Writes bench/results/burst.json.
Times are measured client-side from a single watch stream, so they include API-server latency.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from kubernetes import client, config, watch

NS = "duckdb"
IMAGE = "duckdb-dbt:dev"
OUT = Path(__file__).parent / "results" / "burst.json"


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * q))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pods", type=int, default=30)
    ap.add_argument("--cpu", default="10m", help="request per pod: keep tiny so the burst is about the control plane")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    config.load_kube_config(config_file=os.environ.get("LAUNCHER_KUBECONFIG", ".kube/airflow-launcher.yaml"))
    v1 = client.CoreV1Api()
    run = uuid.uuid4().hex[:6]
    selector = f"bench-burst={run}"

    def body(i: int) -> client.V1Pod:
        return client.V1Pod(
            metadata=client.V1ObjectMeta(name=f"burst-{run}-{i:03d}", labels={"bench-burst": run}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[
                    client.V1Container(
                        name="c",
                        image=IMAGE,
                        image_pull_policy="IfNotPresent",
                        command=["python", "-c", "pass"],
                        resources=client.V1ResourceRequirements(requests={"cpu": args.cpu, "memory": "64Mi"}),
                    )
                ],
            ),
        )

    created: dict[str, float] = {}
    scheduled: dict[str, float] = {}
    running: dict[str, float] = {}
    done: dict[str, float] = {}

    w = watch.Watch()
    t0: list[float] = []

    def create(i: int) -> None:
        pod = body(i)
        created[pod.metadata.name] = time.monotonic()
        v1.create_namespaced_pod(NS, pod)

    def launch() -> None:
        time.sleep(1.0)  # let the watch connect first, so every transition is seen live
        t0.append(time.monotonic())
        with ThreadPoolExecutor(max_workers=min(args.pods, 32)) as pool:
            list(pool.map(create, range(args.pods)))

    launcher = threading.Thread(target=launch)
    launcher.start()
    try:
        for ev in w.stream(v1.list_namespaced_pod, NS, label_selector=selector, timeout_seconds=args.timeout):
            pod, now = ev["object"], time.monotonic()
            name = pod.metadata.name
            if pod.spec.node_name and name not in scheduled:
                scheduled[name] = now
            statuses = pod.status.container_statuses or []
            if statuses and (statuses[0].state.running or statuses[0].state.terminated) and name not in running:
                running[name] = now
            if pod.status.phase in ("Succeeded", "Failed") and name not in done:
                done[name] = now
            if len(done) == args.pods:
                w.stop()
    finally:
        launcher.join()
        # one delete per pod, like KubernetesPodOperator (role has no deletecollection)
        for name in created:
            v1.delete_namespaced_pod(name, NS, grace_period_seconds=0)

    wall = time.monotonic() - t0[0]

    def lat(stage: dict[str, float]) -> dict:
        vals = [stage[n] - created[n] for n in stage if n in created]
        if not vals:
            return {}
        return {"p50_s": round(pct(vals, 0.5), 2), "p95_s": round(pct(vals, 0.95), 2), "max_s": round(max(vals), 2),
                "mean_s": round(statistics.mean(vals), 2)}

    result = {
        "pods": args.pods,
        "completed": len(done),
        "wall_s": round(wall, 2),
        "to_scheduled": lat(scheduled),
        "to_running": lat(running),
        "to_finished": lat(done),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2))

    md = (
        f"### Burst: {args.pods} pods created at once\n\n"
        "| stage (from create call) | p50 s | p95 s | max s |\n|---|---:|---:|---:|\n"
        + "".join(
            f"| {k} | {v.get('p50_s')} | {v.get('p95_s')} | {v.get('max_s')} |\n"
            for k, v in (("scheduled", result["to_scheduled"]), ("container started", result["to_running"]),
                         ("finished", result["to_finished"]))
        )
        + f"\nAll {result['completed']}/{args.pods} finished in {result['wall_s']} s wall-clock.\n"
    )
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)
    return 0 if result["completed"] == args.pods else 1


if __name__ == "__main__":
    sys.exit(main())

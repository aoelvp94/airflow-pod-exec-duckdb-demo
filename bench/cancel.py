"""What happens to the dbt process inside the forever pod when the Airflow side of an exec goes away?

    on_kill   the task is killed cleanly (timeout, marked failed, SIGTERM) -> operator.on_kill()
    sigkill   the worker dies without cleanup (OOM, lost node, dropped connection) -> the socket just closes

For each case: start a slow `dbt show` (it holds DuckDB's write lock) through KubernetesPodExecOperator,
cut it after --kill-after seconds, check whether dbt is still running in the pod, retry a `dbt run`
right away (what an Airflow retry would do), wait for the first dbt to exit, and retry again.
Cosmos' KUBERNETES_EXEC operators inherit KubernetesPodExecOperator.on_kill, so this covers them too.

Records what happened; it does not fail on either outcome. Writes bench/results/cancel.json and
prints Markdown (also to $GITHUB_STEP_SUMMARY).

    just cancel                 # both cases
    uv run python bench/cancel.py on_kill --rows 20000000000
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator

from demo_ops.settings import CONTAINER, DBT, DBT_DIRS, KUBERNETES_CONN_ID, NAMESPACE, POD_NAME

MARKER = "cancel-test"  # in the slow query's SQL, so the dbt process can be found in /proc
OUT = Path(__file__).parent / "results" / "cancel.json"
CONTEXT = os.environ.get("KIND_CONTEXT", "kind-duckdb-exec")
T0 = time.monotonic()


def log(msg: str) -> None:
    print(f"[{time.monotonic() - T0:6.1f}s] {msg}", flush=True)


def op(task_id: str, command: list[str]) -> KubernetesPodExecOperator:
    return KubernetesPodExecOperator(
        task_id=task_id, pod_name=POD_NAME, namespace=NAMESPACE, container_name=CONTAINER,
        command=command, kubernetes_conn_id=KUBERNETES_CONN_ID,
    )


def slow_cmd(rows: int) -> list[str]:
    sql = f"select count(*) as n from range({rows}) t(i) where i % 7 = 3 /* {MARKER} */"
    return [*DBT, "show", *DBT_DIRS, "--inline", sql, "--limit", "1"]


RETRY = [*DBT, "run", "--select", "stg_customers", *DBT_DIRS]


def slow_pids() -> list[int]:
    """PIDs in the pod whose command line carries MARKER (python:slim has no ps, so scan /proc)."""
    script = (
        "for f in /proc/[0-9]*/cmdline; do c=$(tr '\\0' ' ' < $f 2>/dev/null); "
        f"case \"$c\" in *cmdline*) ;; *{MARKER}*) p=${{f#/proc/}}; echo ${{p%/cmdline}};; esac; done"
    )
    for attempt in range(5):  # a busy kind API server times out now and then
        r = subprocess.run(
            ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "exec", POD_NAME, "-c", CONTAINER, "--", "bash", "-c", script],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            return [int(p) for p in r.stdout.split()]
        time.sleep(2 + attempt * 3)
    raise RuntimeError(f"could not list processes in the pod: {r.stderr.strip()}")


def try_run(task_id: str) -> tuple[bool, float]:
    t = time.monotonic()
    try:
        op(task_id, RETRY).execute({})
        ok = True
    except Exception as e:  # noqa: BLE001
        log(f"{task_id} failed: {e}")
        ok = False
    return ok, round(time.monotonic() - t, 1)


def _child(rows: int) -> None:
    op("slow", slow_cmd(rows)).execute({})


def scenario(mode: str, rows: int, kill_after: float) -> dict:
    log(f"--- {mode}")
    if left := slow_pids():
        raise RuntimeError(f"a previous slow dbt is still running in the pod: {left}")

    if mode == "on_kill":
        slow = op("slow", slow_cmd(rows))
        outcome: dict = {}

        def target() -> None:
            try:
                slow.execute({})
                outcome["result"] = "completed"
            except Exception as e:  # noqa: BLE001
                outcome["result"] = f"raised {type(e).__name__}"

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        time.sleep(kill_after)
        before = slow_pids()
        slow.on_kill()
        thread.join(timeout=30)
        client = outcome.get("result", "still blocked after on_kill")
    else:
        child = mp.get_context("spawn").Process(target=_child, args=(rows,))
        child.start()
        time.sleep(kill_after)
        before = slow_pids()
        os.kill(child.pid, signal.SIGKILL)
        child.join()
        client = "killed"
    log(f"cut after {kill_after}s; dbt in pod before the cut: {before}; client side: {client}")
    if not before:
        raise RuntimeError("the slow dbt was not running yet when it was cut; raise --kill-after or --rows")

    time.sleep(3)
    after = slow_pids()
    survived = bool(set(before) & set(after))
    log(f"3 s after the cut, dbt in pod: {after} -> {'still running' if survived else 'gone'}")

    retry_ok, retry_s = try_run("retry_immediately")
    retry_while_orphan = survived and bool(set(before) & set(slow_pids()))

    t = time.monotonic()
    while set(before) & set(slow_pids()):
        time.sleep(5)
    orphan_s = round(time.monotonic() - t, 1)
    log(f"orphan gone {orphan_s}s after the retry")

    later_ok, _ = try_run("retry_after_orphan_exit")
    return {
        "mode": mode,
        "client_side": client,
        "dbt_kept_running": survived,
        "retry_ran_while_orphan_alive": retry_while_orphan,
        "retry_succeeded": retry_ok,
        "retry_s": retry_s,
        "orphan_lived_on_s": orphan_s,
        "retry_after_orphan_exit_succeeded": later_ok,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("modes", nargs="*", metavar="{on_kill,sigkill}", help="default: both")
    ap.add_argument("--rows", type=int, default=20_000_000_000, help="size of the slow query (~1 min on a laptop)")
    ap.add_argument("--kill-after", type=float, default=10.0)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()
    a.modes = a.modes or ["on_kill", "sigkill"]
    if bad := set(a.modes) - {"on_kill", "sigkill"}:
        ap.error(f"unknown mode(s): {sorted(bad)}")

    results = [scenario(m, a.rows, a.kill_after) for m in a.modes]
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(results, indent=2))

    yn = lambda b: "yes" if b else "no"  # noqa: E731
    lines = [
        "### Cancelling an exec: does dbt stop in the pod?",
        "",
        "| cut by | dbt kept running in the pod | immediate retry | dbt lived on after the retry | retry once it exited |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        how = "`operator.on_kill()`" if r["mode"] == "on_kill" else "SIGKILL of the client"
        retry = "succeeded" if r["retry_succeeded"] else "failed" + (" (first dbt still running)" if r["retry_ran_while_orphan_alive"] else "")
        lines.append(
            f"| {how} | {yn(r['dbt_kept_running'])} | {retry} | {r['orphan_lived_on_s']} s | "
            f"{'succeeded' if r['retry_after_orphan_exit_succeeded'] else 'failed'} |"
        )
    md = "\n".join(lines) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

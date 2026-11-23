"""What happens when the exec connection hangs without closing (network partition, frozen NAT/LB entry)?

The operator talks to the API server through a local TCP proxy. Mid-run the proxy stops moving bytes in both
directions but keeps both sockets open: no FIN, no RST, exactly what a silent network partition looks like.
Then it records:

  * the pod side: does dbt finish on its own while nobody reads its output?
  * the client side: does `execute()` notice, or does it block? (waits up to --hang-for seconds)
  * `on_kill()` while hung (what Airflow's execution_timeout does): does execute() return, and how fast?
  * once the network is back: does a retry work?

Writes bench/results/hang.json and prints Markdown (also to $GITHUB_STEP_SUMMARY).

    just hang                         # defaults: freeze after 10 s, wait 150 s before on_kill
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import yaml
from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator

from cancel import RETRY, op as plain_op, slow_cmd, slow_pids  # same slow query and process probe

OUT = Path(__file__).parent / "results" / "hang.json"
KUBECONFIG = Path(__file__).parents[1] / ".kube" / "airflow-exec.yaml"
T0 = time.monotonic()


def log(msg: str) -> None:
    print(f"[{time.monotonic() - T0:6.1f}s] {msg}", flush=True)


class FreezableProxy:
    """Plain TCP relay (TLS passes through untouched). freeze(): stop reading either side, keep sockets open."""

    def __init__(self, upstream: tuple[str, int]) -> None:
        self.upstream = upstream
        self.frozen = threading.Event()
        self.server = socket.create_server(("127.0.0.1", 0))
        self.port = self.server.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            client, _ = self.server.accept()
            up = socket.create_connection(self.upstream)
            for src, dst in ((client, up), (up, client)):
                threading.Thread(target=self._pump, args=(src, dst), daemon=True).start()

    def _pump(self, src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                while self.frozen.is_set():  # don't read: the kernel buffers fill and the peer stalls
                    time.sleep(0.1)
                data = src.recv(65536)
                if not data:
                    break
                while self.frozen.is_set():  # a chunk read just before the freeze waits too
                    time.sleep(0.1)
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


def upstream() -> tuple[str, int]:
    server = urlparse(yaml.safe_load(KUBECONFIG.read_text())["clusters"][0]["cluster"]["server"])
    return server.hostname, server.port


def use_proxy(proxy_port: int) -> None:
    """Airflow conn `hang_k8s`: the exec ServiceAccount's kubeconfig, pointed at the proxy (TLS still verified)."""
    cfg = yaml.safe_load(KUBECONFIG.read_text())
    cfg["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{proxy_port}"
    path = Path(tempfile.mkdtemp()) / "proxied.yaml"
    path.write_text(yaml.safe_dump(cfg))
    os.environ["AIRFLOW_CONN_HANG_K8S"] = json.dumps({"conn_type": "kubernetes", "extra": {"kube_config_path": str(path)}})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20_000_000_000)
    ap.add_argument("--freeze-after", type=float, default=10.0)
    ap.add_argument("--hang-for", type=float, default=150.0, help="how long to wait for execute() before on_kill")
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()

    if left := slow_pids():
        raise RuntimeError(f"a previous slow dbt is still running in the pod: {left}")
    proxy = FreezableProxy(upstream())
    use_proxy(proxy.port)

    slow = KubernetesPodExecOperator(
        task_id="slow", pod_name="duckdb-dbt-0", namespace="duckdb", container_name="dbt",
        command=slow_cmd(a.rows), kubernetes_conn_id="hang_k8s",
    )
    outcome: dict = {}

    def target() -> None:
        try:
            slow.execute({})
            outcome["result"] = "completed"
        except Exception as e:  # noqa: BLE001
            outcome["result"] = f"raised {type(e).__name__}: {str(e)[:120]}"
        outcome["at"] = time.monotonic()

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    time.sleep(a.freeze_after)
    pids = slow_pids()  # direct kubectl, not through the proxy
    if not pids:
        raise RuntimeError("the slow dbt was not running yet when the network froze; raise --freeze-after or --rows")
    proxy.frozen.set()
    t_freeze = time.monotonic()
    log(f"network frozen (no FIN/RST); dbt in pod: {pids}")

    dbt_done_s = None
    while time.monotonic() - t_freeze < a.hang_for and thread.is_alive():
        if dbt_done_s is None and not set(pids) & set(slow_pids()):
            dbt_done_s = round(time.monotonic() - t_freeze, 1)
            log(f"dbt finished in the pod {dbt_done_s}s after the freeze")
        time.sleep(5)
    if dbt_done_s is None and not set(pids) & set(slow_pids()):
        dbt_done_s = round(time.monotonic() - t_freeze, 1)
    noticed = not thread.is_alive()
    log(f"after {round(time.monotonic() - t_freeze)}s frozen, execute() {'returned: ' + outcome.get('result', '') if noticed else 'is still blocked'}")

    on_kill_return_s = None
    if not noticed:
        t_kill = time.monotonic()
        slow.on_kill()
        thread.join(timeout=60)
        on_kill_return_s = round(outcome["at"] - t_kill, 1) if "at" in outcome else None
        log(f"on_kill(): execute() {'returned after ' + str(on_kill_return_s) + 's: ' + outcome.get('result', '') if on_kill_return_s is not None else 'still blocked 60 s later'}")

    proxy.frozen.clear()
    if dbt_done_s is None:
        t = time.monotonic()
        while set(pids) & set(slow_pids()):
            time.sleep(5)
        dbt_done_s = round(time.monotonic() - t_freeze, 1)
        log(f"dbt finished in the pod {dbt_done_s}s after the freeze (after the network came back)")
    try:
        plain_op("retry", RETRY).execute({})
        retry_ok = True
    except Exception as e:  # noqa: BLE001
        log(f"retry failed: {e}")
        retry_ok = False
    log(f"retry after the network came back: {'succeeded' if retry_ok else 'failed'}")

    res = {
        "freeze_after_s": a.freeze_after,
        "hang_for_s": a.hang_for,
        "dbt_finished_in_pod_after_freeze_s": dbt_done_s,
        "execute_noticed_on_its_own": noticed,
        "execute_result": outcome.get("result"),
        "on_kill_returned_after_s": on_kill_return_s,
        "retry_succeeded": retry_ok,
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(res, indent=2))
    md = "\n".join([
        "### Exec connection hangs without closing",
        "",
        "| dbt in the pod | `execute()` on its own | `on_kill()` while hung | retry once the network is back |",
        "|---|---|---|---|",
        f"| finished {dbt_done_s} s after the freeze | "
        + (f"returned: {res['execute_result']}" if noticed else f"still blocked after {a.hang_for:.0f} s")
        + " | "
        + (f"execute() returned after {on_kill_return_s} s" if on_kill_return_s is not None else ("not needed" if noticed else "still blocked"))
        + f" | {'succeeded' if retry_ok else 'failed'} |",
    ]) + "\n"
    print(md)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()

"""PROTOTYPE: Cosmos's watcher protocol over exec, i.e. WATCHER_KUBERNETES's speed with the pod outside Airflow.

How the watcher works today (cosmos.operators.watcher_kubernetes): one producer task runs `dbt build
--log-format json` in a NEW pod; a pod-log callback feeds every log line to
`store_dbt_resource_status_from_log`, which pushes each node's status to XCom; the per-node tasks are
deferrable sensors that wait for their node's status.

This prototype keeps all of that and swaps only the producer: `DbtProducerWatcherKubernetesExecOperator` runs
the same `dbt build` through `KubernetesPodExecOperator` in the existing pod and feeds every stdout line to the
same parser. The sensors are Cosmos's own, unchanged.

How it gets in: Cosmos resolves the producer by import path
(`cosmos.operators.watcher_kubernetes.DbtProducerWatcherKubernetesOperator`), so `exec_producer()` swaps that
attribute while the DbtDag is built. The DAG file runs again on every worker, so the swap applies there too.

Known gaps (fine for measuring, not for shipping):
* The sensors are still KPO subclasses built from the watcher's pod args. Normally they only wait on XCom, but
  a sensor's retry fallback ("run the node on its own") would start a pod, not exec.
* Cancelling the producer leaves dbt running in the pod, as with any exec (see bench/cancel.py).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import cosmos.operators.watcher_kubernetes as watcher_k8s
from cosmos.operators import _k8s_common
from cosmos.operators._watcher.base import store_dbt_resource_status_from_log
from cosmos.operators.kubernetes_exec import DbtBuildKubernetesExecOperator

# What the watcher DAG passes for its NEW pods, meaningless for exec: dropped from the producer's kwargs.
KPO_ONLY = (
    "image", "image_pull_policy", "container_resources", "node_selector", "volumes", "volume_mounts",
    "get_logs", "on_finish_action", "labels", "log_events_on_failure", "deferrable", "callbacks",
    "kubernetes_conn_id", "namespace", "env_vars", "_check_source_freshness",
)


class DbtProducerWatcherKubernetesExecOperator(DbtBuildKubernetesExecOperator):
    """Watcher producer that runs `dbt build` by exec in an existing pod instead of starting one."""

    exec_args: dict[str, Any] = {}  # set by exec_producer(): conn, namespace, pod, container, pool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for key in KPO_ONLY:
            kwargs.pop(key, None)
        kwargs.update(self.exec_args)
        kwargs["do_xcom_push"] = False  # the JSON log of a big build would exceed the XCom output limit
        task_id = _k8s_common.init_watcher_producer(self, kwargs)
        kwargs.pop("callbacks", None)  # the pod-log callback it injects doesn't apply: we parse stdout below
        super().__init__(task_id=task_id, *args, **kwargs)
        _k8s_common.finalize_watcher_producer(self)  # --log-format json, XCom backup on failure

    def execute(self, context: Any, **kwargs: Any) -> Any:
        parent_execute = super().execute
        return _k8s_common.execute_watcher_producer(self, context, parent_execute, **kwargs)

    def _log_lines(self, lines: Sequence[str], *, stream_name: str) -> None:
        super()._log_lines(lines, stream_name=stream_name)
        if stream_name != "stdout":
            return
        context = self._context_holder.get(_k8s_common.CONTEXT_KEY)
        for line in lines:
            store_dbt_resource_status_from_log(
                line.removesuffix("\r"),
                {"context": context} if context else {},
                tests_per_model=self._tests_per_model,
                test_results_per_model=self._test_results_per_model,
                model_outlet_uris=self._model_outlet_uris,
                should_generate_model_uris=self._should_generate_model_uris,
                upstream_failure_skipped_ids=self._upstream_failure_skipped_ids,
            )


@contextmanager
def exec_producer(**exec_args: Any) -> Iterator[None]:
    """Build WATCHER_KUBERNETES DbtDags inside this block to get the exec producer instead of the pod one."""
    DbtProducerWatcherKubernetesExecOperator.exec_args = exec_args
    original = watcher_k8s.DbtProducerWatcherKubernetesOperator
    watcher_k8s.DbtProducerWatcherKubernetesOperator = DbtProducerWatcherKubernetesExecOperator
    try:
        yield
    finally:
        watcher_k8s.DbtProducerWatcherKubernetesOperator = original

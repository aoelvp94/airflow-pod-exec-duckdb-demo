"""Runs AksPodExecOperator against the local kind cluster with only the Azure calls mocked.

"AKS" returns kind's kubeconfig; "Entra ID" returns a short-lived ServiceAccount token.
Everything else — token swap, temp kubeconfig, RBAC, the real websocket exec — is real.
Requires `make up`.
"""

from __future__ import annotations

import subprocess
from unittest import mock

import pytest

from demo_ops import aks_pod_exec
from demo_ops.aks_pod_exec import AksPodExecOperator
from demo_ops.settings import CONTAINER, NAMESPACE, POD_NAME

CLUSTER = "duckdb-exec"


def _sh(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()


@pytest.fixture
def mocked_azure():
    kubeconfig = _sh("kind", "get", "kubeconfig", "--name", CLUSTER)
    token = _sh(
        "kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE,
        "create", "token", "airflow-exec", "--duration=10m",
    )
    with (
        mock.patch.object(aks_pod_exec, "ContainerServiceClient") as aks,
        mock.patch.object(aks_pod_exec, "DefaultAzureCredential") as cred,
    ):
        creds = mock.MagicMock()
        creds.kubeconfigs = [mock.MagicMock(value=kubeconfig.encode())]
        aks.return_value.managed_clusters.list_cluster_user_credentials.return_value = creds
        cred.return_value.get_token.return_value = mock.MagicMock(token=token)
        yield aks


def _op(**overrides) -> AksPodExecOperator:
    kwargs = dict(
        task_id="aks_exec",
        subscription_id="00000000-0000-0000-0000-000000000000",
        resource_group="rg-fake",
        cluster_name="aks-fake",
        namespace=NAMESPACE,
        pod_name=POD_NAME,
        container_name=CONTAINER,
        command=["python", "-c", "import duckdb; print(duckdb.__version__)"],
        do_xcom_push=True,
    )
    kwargs.update(overrides)
    return AksPodExecOperator(**kwargs)


def test_exec_through_aks_flavour(mocked_azure):
    out = _op().execute(context={})
    assert out.strip().lstrip("v")[0].isdigit()  # a DuckDB version string came back as XCom
    mocked_azure.return_value.managed_clusters.list_cluster_user_credentials.assert_called_once_with(
        "rg-fake", "aks-fake"
    )


def test_rbac_is_namespaced(mocked_azure):
    """The ServiceAccount may exec in `duckdb` only: same blast radius you'd want on AKS."""
    with pytest.raises(Exception, match="(?i)403|forbidden"):
        _op(
            namespace="kube-system",
            pod_name=f"etcd-{CLUSTER}-control-plane",
            container_name=None,
            command=["true"],
        ).execute(context={})

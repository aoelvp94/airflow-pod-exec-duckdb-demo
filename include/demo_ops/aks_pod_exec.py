"""A home-made AKS flavour of KubernetesPodExecOperator (Airflow has none upstream).

Mirrors what EksPodExecOperator / GKEPodExecOperator add on top of the generic operator:
discover the cluster from the cloud API and mint a short-lived token at run time,
so no kubeconfig secret or `kubelogin` binary is needed on the workers.
"""

from __future__ import annotations

import tempfile

import yaml
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerservice import ContainerServiceClient

from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator

# Fixed Entra ID application for the AKS API server (same for every AAD-enabled cluster).
AKS_AAD_SCOPE = "6dae42f8-4368-4678-94ff-3960e28e3630/.default"


class AksPodExecOperator(KubernetesPodExecOperator):
    template_fields = (*KubernetesPodExecOperator.template_fields, "resource_group", "cluster_name")

    def __init__(self, *, subscription_id: str, resource_group: str, cluster_name: str, **kwargs) -> None:
        super().__init__(kubernetes_conn_id=None, **kwargs)
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.cluster_name = cluster_name

    def _build_kubeconfig(self) -> dict:
        credential = DefaultAzureCredential()
        aks = ContainerServiceClient(credential, self.subscription_id)
        raw = (
            aks.managed_clusters.list_cluster_user_credentials(self.resource_group, self.cluster_name)
            .kubeconfigs[0]
            .value
        )
        kubeconfig = yaml.safe_load(raw)
        # Swap the kubelogin exec plugin (or client cert) for a fresh bearer token.
        token = credential.get_token(AKS_AAD_SCOPE).token
        kubeconfig["users"] = [{"name": "airflow", "user": {"token": token}}]
        for ctx in kubeconfig["contexts"]:
            ctx["context"]["user"] = "airflow"
        return kubeconfig

    def execute(self, context):
        kubeconfig = self._build_kubeconfig()
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
            yaml.safe_dump(kubeconfig, f)
            f.flush()
            self.config_file = f.name  # hook is a lazy cached_property: built after this line
            return super().execute(context)

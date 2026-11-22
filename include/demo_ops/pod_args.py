"""What a NEW pod gets in the modes that launch pods (KubernetesPodOperator, Cosmos KUBERNETES / WATCHER_KUBERNETES):
the same image, requests and data volume as the forever pod, so both sides of a comparison are sized alike."""

from __future__ import annotations

from kubernetes.client import (
    V1PersistentVolumeClaimVolumeSource,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

from demo_ops.settings import IMAGE, LAUNCHER_CONN_ID, NAMESPACE, POD_CPU_REQUEST, POD_MEMORY_REQUEST

NEW_POD_ARGS = {
    "kubernetes_conn_id": LAUNCHER_CONN_ID,
    "namespace": NAMESPACE,
    "image": IMAGE,
    "image_pull_policy": "IfNotPresent",
    "container_resources": V1ResourceRequirements(
        requests={"cpu": POD_CPU_REQUEST, "memory": POD_MEMORY_REQUEST},
        limits={"memory": "2Gi"},
    ),
    "node_selector": {"kubernetes.io/os": "linux"},
    "volumes": [
        V1Volume(
            name="data",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name="data-duckdb-dbt-0"),
        )
    ],
    "volume_mounts": [V1VolumeMount(name="data", mount_path="/data")],
    "get_logs": True,
    "on_finish_action": "delete_pod",
}

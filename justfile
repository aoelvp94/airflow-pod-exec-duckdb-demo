set shell := ["bash", "-euo", "pipefail", "-c"]

cluster := "duckdb-exec"
ctx     := "kind-" + cluster
image   := "duckdb-dbt:dev"
ns      := "duckdb"
kubectl := "kubectl --context " + ctx

export AIRFLOW_HOME := justfile_directory() / ".airflow"
export AIRFLOW__CORE__DAGS_FOLDER := justfile_directory() / "dags"
export AIRFLOW__CORE__LOAD_EXAMPLES := "False"
export PYTHONPATH := justfile_directory() / "include"
# Airflow talks to kind as the least-privilege ServiceAccount, not as cluster-admin.
export AIRFLOW_CONN_DUCKDB_K8S := '{"conn_type": "kubernetes", "extra": {"kube_config_path": "' + justfile_directory() / ".kube/airflow-exec.yaml" + '"}}'

# list recipes
default:
    @just --list

# everything from scratch: cluster, image, pod, kubeconfig, Airflow DB
up: cluster image deploy kubeconfig airflow-init

# create the kind cluster (stand-in for AKS / EKS / GKE)
cluster:
    kind get clusters | grep -qx {{cluster}} || kind create cluster --config kind/cluster.yaml

# build the dbt + DuckDB image and load it into kind
image:
    docker build -t {{image}} -f image/Dockerfile .
    kind load docker-image {{image}} --name {{cluster}}

# apply namespace, RBAC and the long-lived StatefulSet
deploy:
    {{kubectl}} apply -f k8s/
    {{kubectl}} -n {{ns}} rollout status statefulset/duckdb-dbt --timeout=180s

# write .kube/airflow-exec.yaml holding a 24h token for the airflow-exec ServiceAccount
kubeconfig:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .kube
    server=$({{kubectl}} config view --minify -o jsonpath='{.clusters[0].cluster.server}')
    ca=$({{kubectl}} config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
    token=$({{kubectl}} -n {{ns}} create token airflow-exec --duration=24h)
    cat > .kube/airflow-exec.yaml <<EOF
    apiVersion: v1
    kind: Config
    current-context: airflow-exec
    clusters: [{name: kind, cluster: {server: '$server', certificate-authority-data: '$ca'}}]
    users: [{name: airflow-exec, user: {token: '$token'}}]
    contexts: [{name: airflow-exec, context: {cluster: kind, user: airflow-exec, namespace: {{ns}}}}]
    EOF
    echo "wrote .kube/airflow-exec.yaml"

# install Airflow, migrate its DB, create the single-writer pool
airflow-init:
    uv sync
    uv run airflow db migrate
    uv run airflow pools set duckdb_writer 1 "DuckDB single writer"

# run the DAG once, in-process, against the kind pod
run:
    uv run airflow dags test duckdb_dbt_exec

# ad-hoc query straight into the pod: just query "select * from customer_revenue"
query sql:
    {{kubectl}} -n {{ns}} exec duckdb-dbt-0 -- python /app/bin/query.py "{{sql}}"

# open a shell in the forever pod
shell:
    {{kubectl}} -n {{ns}} exec -it duckdb-dbt-0 -- bash

# AKS-flavoured operator tests (Azure mocked, kind real)
test:
    uv run pytest -v

# ship a changed dbt project: rebuild image, restart pod (PVC data survives)
reload: image
    {{kubectl}} -n {{ns}} rollout restart statefulset/duckdb-dbt
    {{kubectl}} -n {{ns}} rollout status statefulset/duckdb-dbt --timeout=180s

# delete the kind cluster
down:
    kind delete cluster --name {{cluster}}
    rm -f .kube/airflow-exec.yaml

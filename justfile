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
# Airflow talks to kind as least-privilege ServiceAccounts, not as cluster-admin.
export AIRFLOW_CONN_DUCKDB_K8S := '{"conn_type": "kubernetes", "extra": {"kube_config_path": "' + justfile_directory() / ".kube/airflow-exec.yaml" + '"}}'
# Benchmark only: may create pods (KubernetesPodOperator side of the comparison).
export AIRFLOW_CONN_DUCKDB_K8S_LAUNCHER := '{"conn_type": "kubernetes", "extra": {"kube_config_path": "' + justfile_directory() / ".kube/airflow-launcher.yaml" + '"}}'
export LAUNCHER_KUBECONFIG := justfile_directory() / ".kube/airflow-launcher.yaml"

# list recipes
default:
    @just --list

# everything from scratch: cluster, image, pod, kubeconfig, Airflow DB
up: cluster image deploy kubeconfigs airflow-init

# create the kind cluster (stand-in for AKS / EKS / GKE)
cluster:
    kind get clusters | grep -qx {{cluster}} || kind create cluster --config kind/cluster.yaml

# build the dbt + DuckDB image and load it into kind
image:
    docker build -t {{image}} -f image/Dockerfile .
    kind load docker-image {{image}} --name {{cluster}}
    just manifest

# copy the dbt manifest out of the image, so Cosmos renders exactly what the pod will run
manifest:
    mkdir -p dbt/target
    docker run --rm --entrypoint cat {{image}} /app/dbt/target/manifest.json > dbt/target/manifest.json

# apply namespace, RBAC and the long-lived StatefulSet
deploy:
    {{kubectl}} apply -f k8s/
    {{kubectl}} -n {{ns}} rollout status statefulset/duckdb-dbt --timeout=180s

# kubeconfigs for both ServiceAccounts (exec-only, and the benchmark's pod launcher)
kubeconfigs: (kubeconfig "airflow-exec") (kubeconfig "airflow-launcher")

# write .kube/<sa>.yaml holding a 24h token for that ServiceAccount
kubeconfig sa="airflow-exec":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .kube
    server=$({{kubectl}} config view --minify -o jsonpath='{.clusters[0].cluster.server}')
    ca=$({{kubectl}} config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
    token=$({{kubectl}} -n {{ns}} create token {{sa}} --duration=24h)
    cat > .kube/{{sa}}.yaml <<EOF
    apiVersion: v1
    kind: Config
    current-context: {{sa}}
    clusters: [{name: kind, cluster: {server: '$server', certificate-authority-data: '$ca'}}]
    users: [{name: {{sa}}, user: {token: '$token'}}]
    contexts: [{name: {{sa}}, context: {cluster: kind, user: {{sa}}, namespace: {{ns}}}}]
    EOF
    echo "wrote .kube/{{sa}}.yaml"

# install Airflow, migrate its DB, create the single-writer pool
airflow-init:
    uv sync
    uv run airflow db migrate
    uv run airflow pools set duckdb_writer 1 "DuckDB single writer"

# run the DAG once, in-process, against the kind pod; fails if any task failed
run:
    uv run airflow dags test duckdb_dbt_exec
    uv run python bench/check_run.py duckdb_dbt_exec

# latency benchmark: n tasks per group, exec vs pod-per-task, then the at-scale estimate
bench n="10":
    BENCH_N={{n}} uv run airflow dags test bench_exec_vs_kpo
    uv run python bench/report.py
    uv run python bench/estimate.py

# same dbt project three ways: hand-made exec, Cosmos KUBERNETES_EXEC, Cosmos KUBERNETES (runs each N times)
compare runs="3":
    #!/usr/bin/env bash
    set -euo pipefail
    for dag in duckdb_dbt_exec cosmos_dbt_kubernetes_exec cosmos_dbt_kubernetes; do
      for i in $(seq {{runs}}); do
        uv run airflow dags test "$dag" > /dev/null 2>&1 || true
        uv run python bench/check_run.py "$dag"
      done
    done
    uv run python bench/compare.py --runs {{runs}}

# burst: create many pods at once and time scheduling/start/finish
burst pods="30":
    uv run python bench/burst.py --pods {{pods}}

# re-run only the at-scale estimate with your own assumptions: just estimate --work-s 45 --tasks 500 5000
estimate *args:
    uv run python bench/estimate.py {{args}}

# any Airflow CLI command with this repo's env: just airflow dags list
airflow *args:
    uv run airflow {{args}}

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
    rm -f .kube/airflow-exec.yaml .kube/airflow-launcher.yaml

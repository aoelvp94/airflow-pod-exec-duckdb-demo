set shell := ["bash", "-euo", "pipefail", "-c"]

cluster := "duckdb-exec"
ctx     := "kind-" + cluster
image   := "duckdb-dbt:dev"
# synthetic project sizes baked into the image for `just scale` (scale/gen.py)
scale_sizes := env_var_or_default("SCALE_SIZES", "10 50 200")
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
    docker build -t {{image}} -f image/Dockerfile --build-arg SCALE_SIZES="{{scale_sizes}}" .
    kind load docker-image {{image}} --name {{cluster}}
    just manifest scale-manifests

# copy the dbt manifest out of the image, so Cosmos renders exactly what the pod will run
manifest:
    mkdir -p dbt/target
    docker run --rm --entrypoint cat {{image}} /app/dbt/target/manifest.json > dbt/target/manifest.json

# same for the synthetic projects: scale/target/scale_<N>/manifest.json
scale-manifests:
    #!/usr/bin/env bash
    set -euo pipefail
    for n in {{scale_sizes}}; do
      mkdir -p scale/target/scale_$n
      docker run --rm --entrypoint cat {{image}} /app/scale/scale_$n/target/manifest.json > scale/target/scale_$n/manifest.json
    done

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

# same dbt project FOUR ways (adds Cosmos WATCHER_KUBERNETES) through a real local scheduler + triggerer
compare-scheduled runs="3":
    #!/usr/bin/env bash
    set -euo pipefail
    since=$(date -u '+%Y-%m-%d %H:%M:%S')
    uv run python bench/scheduled.py --runs {{runs}} \
      duckdb_dbt_exec cosmos_dbt_kubernetes_exec cosmos_dbt_kubernetes cosmos_dbt_watcher_kubernetes cosmos_dbt_watcher_exec
    uv run python bench/compare.py --runs {{runs}} --since "$since" --out bench/results/compare-scheduled.json

# cut an exec mid-run (on_kill, then SIGKILL of the client): does dbt keep running in the pod? does a retry work?
cancel *args:
    uv run python bench/cancel.py {{args}}

# where a Cosmos KUBERNETES_EXEC task spends its time: one run of an N-model project, broken into segments
profile-exec size="50":
    #!/usr/bin/env bash
    set -euo pipefail
    export PROFILE_EXEC_SIZE={{size}}
    uv run python bench/profile_exec.py install
    uv run python bench/scheduled.py --runs 1 --run-timeout 3600 profile_{{size}}_kubernetes_exec
    uv run python bench/profile_exec.py report --dag profile_{{size}}_kubernetes_exec

# freeze the exec connection mid-run without closing it (no FIN/RST): does execute() notice? does on_kill help?
hang *args:
    uv run python bench/hang.py {{args}}

# P concurrent dbt builds in the pod: shared target/logs vs one workspace each
parallel *args:
    uv run python bench/parallel.py {{args}}

# KUBERNETES (a pod per model) runs only up to kpo_max models: at 200 it would take ~25 min on its own.
# how each mode grows with project size: synthetic projects of N models, 4 ways each, real scheduler
scale runs="1" kpo_max="50":
    #!/usr/bin/env bash
    set -euo pipefail
    export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300 AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=300
    uv run python bench/scale_stubs.py write {{scale_sizes}}  # one DAG per file
    trap 'uv run python bench/scale_stubs.py clean' EXIT
    uv run python bench/scale_parse.py {{scale_sizes}}
    dags=()
    for n in {{scale_sizes}}; do
      dags+=(scale_${n}_build scale_${n}_watcher scale_${n}_watcher_exec scale_${n}_kubernetes_exec)
      if [ "$n" -le {{kpo_max}} ]; then dags+=(scale_${n}_kubernetes); fi
    done
    since=$(date -u '+%Y-%m-%d %H:%M:%S')
    uv run python bench/scheduled.py --runs {{runs}} --run-timeout 3600 "${dags[@]}"
    uv run python bench/compare.py --runs {{runs}} --since "$since" --dags "${dags[@]}" \
      --out bench/results/compare-scale.json --title "Synthetic projects by size, four ways (whole DAG run)"
    uv run python bench/scale_report.py

# run given DAGs n times each through a real local scheduler: just scheduled 1 cosmos_dbt_watcher_exec
scheduled runs +dags:
    uv run python bench/scheduled.py --runs {{runs}} --run-timeout 1800 {{dags}}

# any repo script with this repo's env, arguments passed as given: just py bench/compare.py --since "2026-11-26 15:00:00"
[positional-arguments]
py *args:
    uv run python "$@"

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

# airflow-pod-exec-duckdb-demo

Run dbt and ad-hoc SQL inside **one long-lived DuckDB pod** from Airflow, using
`KubernetesPodExecOperator` (cncf.kubernetes provider ≥ 10.22) instead of starting a new pod per task.
Everything runs locally on [kind](https://kind.sigs.k8s.io/), which stands in for AKS / EKS / GKE.

```
 Airflow (local, uv)                         kind cluster
 ───────────────────                         ───────────────────────────────────────────
 duckdb_dbt_exec DAG                          ns: duckdb
   dbt_seed ───────── exec ───────────┐       ┌──────────────────────────────────────┐
   dbt_build ──────── exec ───────────┼──────▶│ duckdb-dbt-0  (StatefulSet, 1 replica)│
   query_customer_revenue ─ exec ─────┘       │   dbt-duckdb + parsed dbt project    │
   report (local, reads XCom)                 │   /data/warehouse.duckdb  ◀── PVC    │
                                              └──────────────────────────────────────┘
 identity: ServiceAccount `airflow-exec` (get pods, pods/exec in `duckdb` only)
```

## Quick start

Needs Docker, kind, kubectl, uv and just.

```bash
just up        # kind cluster, image, pod, least-privilege kubeconfig, Airflow DB
just run       # airflow dags test duckdb_dbt_exec
just query "select * from customer_revenue order by revenue desc"
just test      # AKS-flavoured operator, Azure mocked, kind real
just down
```

Change a model, then `just reload` (rebuilds the image and restarts the pod; the PVC keeps the warehouse).

## Why a "forever pod"?

| | Pod per task (KPO / Cosmos KUBERNETES) | Exec into a long-lived pod (this repo) |
|---|---|---|
| Start-up per task | schedule + image pull + Python/dbt import + parse: ~10–60 s | ~1 s: process start in a warm container |
| DuckDB file | must be fetched/attached every time (object storage, RWX volume) | stays on the pod's PVC |
| Isolation | full: each task gets its own CPU/memory, crashes are contained | shared: a runaway query affects every task |
| Concurrency | scales out | **one writer at a time** (DuckDB is single-writer) → `duckdb_writer` pool, 1 slot |
| Sizing | per task | fixed by the pod's requests/limits |
| Failure mode | task pod fails, retries in a fresh pod | pod restart kills in-flight execs; tasks retry when it's back |
| Deploying dbt changes | new image tag per run | roll the StatefulSet (`just reload`) |

It's a good fit when you have many small, fast dbt/SQL steps against one embedded DuckDB file, and
pod start-up costs more than the work itself. It's a poor fit when tasks need isolation, run in parallel
with heavy writes, or need very different resource sizes. In those cases use KPO, or put DuckDB behind
something with a server (MotherDuck, or a DuckLake / Iceberg catalog that many readers can share).

Note: DuckDB is embedded, not a server. The pod doesn't serve queries. Each exec starts a short-lived
`dbt` or `python` process that opens the file. Read-only queries can run alongside each other, but a
writer needs the file to itself, which is why writers share a one-slot pool.

## Layout

```
dags/duckdb_dbt_exec.py          the DAG (exec tasks + XCom)
include/demo_ops/settings.py     conn id, pod, pool, dbt flags
include/demo_ops/aks_pod_exec.py home-made AksPodExecOperator (Airflow has no AKS flavour)
tests/test_aks_pod_exec.py       runs it against kind with only the Azure SDK calls mocked
image/                           dbt + DuckDB image, dbt project pre-parsed into it
dbt/                             seeds → staging views → customer_revenue mart + tests
k8s/                             namespace, RBAC, StatefulSet + PVC
justfile                         every command above
```

## Mapping to the clouds

- **EKS:** swap the connection for `EksPodExecOperator(cluster_name=..., aws_conn_id=...)`.
- **GKE:** `GKEPodExecOperator(cluster_name=..., location=..., project_id=...)`.
- **AKS:** no upstream operator yet. `AksPodExecOperator` in `include/` gets the cluster's kubeconfig
  through the Azure SDK and swaps in a fresh Entra token. It's tested here with mocks only, so run it
  against a real AKS cluster once before relying on it.

There's no emulator for the cloud control planes. Azurite only emulates Azure Storage (Blob/Queue/Table),
not AKS. LocalStack can emulate EKS on its paid tier. That's why the Kubernetes side here is a real kind
cluster and only the cloud auth calls are mocked.

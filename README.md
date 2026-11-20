# airflow-pod-exec-duckdb-demo

[![e2e on kind](https://github.com/aoelvp94/airflow-pod-exec-duckdb-demo/actions/workflows/e2e.yml/badge.svg)](https://github.com/aoelvp94/airflow-pod-exec-duckdb-demo/actions/workflows/e2e.yml)

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
just bench 10  # exec vs pod-per-task latency + at-scale estimate
just burst 50  # 50 pods created at once
just down
```

CI ([`.github/workflows/e2e.yml`](.github/workflows/e2e.yml)) runs all of the above on a fresh kind cluster on
every push and PR. The benchmark tables appear in the run's job summary and `bench/results/` is uploaded as an
artifact. To change the size, run it manually with the `bench_n` / `burst_pods` inputs.

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

## Measuring it

Three tools, from measured to estimated:

1. **`just bench N`** runs the DAG [`bench_exec_vs_kpo`](dags/bench_exec_vs_kpo.py): the same work N times as
   exec into the forever pod and N times as a `KubernetesPodOperator` pod with the same image and requests.
   Two workloads: `noop` (`python -c pass`, pure overhead) and `dbt` (`dbt build`, 2 seeds + 3 models + 5 tests).
   Tasks run one after another, so each duration is single-task latency, not contention.
   [`bench/report.py`](bench/report.py) reads durations from the Airflow DB.
2. **`just burst K`** ([`bench/burst.py`](bench/burst.py)) creates K pods at once, as a wide fan-out of KPO tasks
   would, and times create → scheduled → container started → finished from one watch stream.
3. **`just estimate …`** ([`bench/estimate.py`](bench/estimate.py)) scales the measured overheads to tasks/day.
   It adds the costs kind can't show (cold image pulls, waiting for a new node) as explicit assumptions and prices
   both options (Fargate list prices by default; override them). It prints which inputs were measured and which assumed.

Local reference numbers (Apple Silicon laptop, Docker Desktop, single-node kind, image already on the node: the
**best case** for pod-per-task). These are from a short run, so check them against the CI job summary:

| p50 per task | exec into forever pod | new pod per task (KPO) |
|---|---:|---:|
| noop | 0.1 s | 5.5 s |
| `dbt build` | 5.7 s | 11.2 s |

| burst | container started p50 / p95 | all finished |
|---|---:|---:|
| 30 pods | 2.5 s / 3.5 s | 5.0 s |
| 100 pods | 11.9 s / 14.3 s | 18.7 s |

Reading the estimate:

- **Latency is the clear win.** A new pod costs about 5 s even in the best case. In the cloud, add a cold image
  pull now and then, plus ~60 s when the autoscaler has to add a node. On a DAG with 10 sequential tasks, that's
  minutes per run.
- **Cost is not automatically a win.** A forever pod reserves its CPU and memory 24/7. With short tasks, paying
  per pod-second is cheaper until roughly 10k+ tasks/day (run `just estimate` with your numbers).
- **The single writer caps throughput.** One pod can run at most `86400 / task seconds` writing tasks per day.
  Near that limit you need several DuckDB files/pods (shard by domain) or a real multi-writer engine.
- **At scale, the control plane suffers first.** The burst shows start latency growing with the number of pods
  started at once (30 → 100 pods: p50 2.5 s → 12 s on one node). Thousands of short pods also mean API-server
  and etcd churn, events, and pod IPs. A forever pod creates none of that.
- **Under KubernetesExecutor**, every Airflow task already runs in a worker pod, so exec saves the *second*
  pod (worker + KPO pod → worker only), not all pod overhead.

Don't benchmark on a laptop with the lid closed: macOS sleeps on battery even under `caffeinate`, and tasks
come out at 30 s or 1,000 s. CI is the reliable place to run it.

## Layout

```
dags/duckdb_dbt_exec.py          the DAG (exec tasks + XCom)
dags/bench_exec_vs_kpo.py        benchmark DAG: exec vs KubernetesPodOperator, same work
bench/                           report, burst test, at-scale estimator, run checker
.github/workflows/e2e.yml        CI: everything above on kind
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

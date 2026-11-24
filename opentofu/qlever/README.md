# QLever on GKE

QLever runs as a single pod on an autoscaled zonal GKE cluster (1–2 nodes).

- **Nodes:** a qlever pod reserves most of a node's memory, so each node runs only one. Normally one node serves. During a rollout the new pod doesn't fit on it, so the cluster autoscaler adds a second node for it and removes the empty node about 10 minutes after the rollout. The pod is annotated `safe-to-evict: "false"` so the autoscaler never moves the serving pod.
- **Storage:** each pod gets its own new `pd-ssd` from the `qlever-ssd` StorageClass, as a generic ephemeral volume. Kubernetes creates the disk with the pod and deletes it with the pod.
- **Server:** `qlever-server` serves the index from that disk and preloads every materialized view in it.
- **Ingress:** a GKE Ingress on a global static IP serves every hostname in `qlever_url` over HTTPS with one Google-managed certificate.

## Serving a new index

Every pod starts on an empty disk. Its `sync-index` init container copies `gs://<index_bucket>/geoconnex_index` onto the disk, and qlever starts once the copy finishes.

After `stream_qlever_index_to_gcs` uploads a new index, the `restart_qlever_on_gke` Dagster asset restarts the deployment and waits up to an hour for the rollout to finish. Materialize `check_qlever_gke_connection` first to confirm that Dagster can reach the cluster; `restart_qlever_on_gke` depends on it.

Dagster authenticates as the Dagster VM's service account. That account needs permission to update deployments in the cluster, for example `roles/container.developer`.

To restart by hand:

```sh
gcloud container clusters get-credentials <vm_name>-qlever --zone <zone>
kubectl -n qlever rollout restart deployment/qlever
kubectl -n qlever rollout status deployment/qlever
kubectl -n qlever logs -f -l app=qlever -c sync-index   # then: -c qlever
```

The new index loads whatever views it contains, so adding a view to `assets/materialized_views.json` doesn't need a `tofu apply`.

### Downtime

A restart is a rolling update with no downtime:

1. The autoscaler adds a second node (a few minutes), and the new pod starts there with its own disk, then syncs and loads the index.
2. Once the new pod is ready and healthy behind the load balancer, the old pod is taken out of the load balancer.
3. The old pod keeps serving for `drain_seconds` (30 s) and then gets up to the 300 s query timeout to finish running queries.
4. The old pod and its disk are deleted, and the autoscaler later removes the now empty node.

These restarts still cause downtime:

- **Restarts GKE triggers itself:** a node auto-upgrade or auto-repair evicts the pod before its replacement is ready. qlever is down until the new pod has synced and loaded. The cluster's maintenance window limits automatic upgrades to 07:00–11:00 UTC (2–6am US Eastern in winter, 3–7am in summer); auto-repair can still happen at any time.
- **A restart during an upload:** the new pod syncs whatever is in the bucket at the time, which may be a partial index. `restart_qlever_on_gke` only runs after the upload finishes.

## First apply

The kubernetes provider can't plan the workload until the cluster exists, so the first apply happens in steps:

```sh
# 1. destroy the old qlever VM and create the cluster and global IP
tofu apply -target=module.instances -target=module.qlever.google_container_node_pool.qlever -target=module.qlever.google_compute_global_address.qlever

# 2. point the DNS A record of every hostname in qlever_url at the new IP
tofu output qlever_ip

# 3. create the qlever workload and Ingress
tofu apply
```

Google issues the managed certificate only after DNS resolves to the new IP, which can take 15–60 minutes. Check on it with `kubectl -n qlever describe managedcertificate qlever`.

After the first apply, a normal `tofu apply` works.

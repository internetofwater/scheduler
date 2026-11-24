# QLever on GKE

QLever runs as a single pod on a one-node zonal GKE cluster:

- a `pd-ssd` persistent disk (`google_compute_disk.qlever_data`) holds the index under `index/`.
- `qlever-server` serves the index from the disk and always preloads every materialized view in it.
- a GKE Ingress on a global static IP serves every hostname in `qlever_url` over HTTPS with one Google-managed certificate.

## Serving a new index

Every time the qlever pod starts, its `sync-index` init container runs `gcloud storage rsync` to copy `gs://<index_bucket>/geoconnex_index` onto the persistent disk. QLever starts after the sync finishes. To serve a new index once the `stream_qlever_index_to_gcs` export has finished, restart the deployment:

```sh
gcloud container clusters get-credentials <vm_name>-qlever --zone <zone>
kubectl -n qlever rollout restart deployment/qlever
kubectl -n qlever logs -f deployment/qlever -c sync-index   # then: -c qlever
```

QLever is down from the restart until the new index has synced and loaded. After `stream_qlever_index_to_gcs` finishes, the `restart_qlever_on_gke` asset restarts the deployment and waits up to an hour for the new pod to become available. It authenticates as the Dagster VM's service account, which needs permission to update deployments in the cluster (e.g. `roles/container.developer`).

A restart during an upload syncs the partial index and deletes files from the disk that aren't in the bucket yet. GKE can also restart the pod on its own, for example during a node auto-upgrade or auto-repair, and that restart syncs whatever is in the bucket at the time. Set `sync_index_on_start = false` to turn off the sync and serve only what is already on the disk.

The restarted pod loads whatever views are in the new index, so adding a view to `assets/materialized_views.json` doesn't need a `tofu apply`.

## First apply / migrating from the VM

The kubernetes provider needs the cluster to exist before it can plan the workload, so the first apply takes two steps:

```sh
# 1. destroy the old qlever VM and create the cluster, disk and global IP
tofu apply -target=module.instances -target=module.qlever.google_container_node_pool.qlever -target=module.qlever.google_compute_disk.qlever_data -target=module.qlever.google_compute_global_address.qlever

# 2. point the DNS A record of every hostname in qlever_url at the new IP
tofu output qlever_ip

# 3. create the qlever workload and Ingress
tofu apply
```

The managed certificate is only issued after DNS resolves to the new IP, which can take 15–60 minutes. Check on it with `kubectl -n qlever describe managedcertificate qlever`.

After that, a normal `tofu apply` works.

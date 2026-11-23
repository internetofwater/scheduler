# QLever on GKE

QLever runs as a single pod on a one-node zonal GKE cluster:

- a `pd-ssd` persistent disk (`google_compute_disk.qlever_data`) holds the index under `index/`.
- `qlever-server` serves the index from the disk and always preloads every materialized view in it.
- a GKE Ingress on a global static IP serves every hostname in `qlever_url` over HTTPS with one Google-managed certificate.

The managed certificate is only issued after DNS resolves to the new IP, which can take 15–60 minutes. Check on it with `kubectl -n qlever describe managedcertificate qlever`.

After that, a normal `tofu apply` works.

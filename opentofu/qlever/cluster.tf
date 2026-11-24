# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# A zonal GKE cluster that serves the qlever index. It has two nodes so a new
# qlever pod can sync and load on one while the old pod keeps serving on the other.
# The workload itself is defined in kubernetes.tf

resource "google_container_cluster" "qlever" {
  name     = "${var.name}-qlever"
  location = var.zone
  network  = var.network_name

  deletion_protection = false

  # the qlever node pool below holds the nodes; drop the default pool GKE
  # creates with the cluster
  remove_default_node_pool = true
  initial_node_count       = 1

  # VPC native networking with auto created secondary ranges
  ip_allocation_policy {}

  release_channel {
    channel = "REGULAR"
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }
}

resource "google_container_node_pool" "qlever" {
  name       = "qlever"
  cluster    = google_container_cluster.qlever.id
  location   = var.zone
  node_count = var.node_count

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = var.machine_type
    disk_type    = "pd-balanced"
    disk_size_gb = 50

    service_account = var.service_account_email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

# The Ingress load balancer is global, so it needs a global address;
# the DNS A record of each qlever_url must point here
resource "google_compute_global_address" "qlever" {
  name = "${var.name}-qlever-ip"
}

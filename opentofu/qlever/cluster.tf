# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# A zonal GKE cluster that serves the qlever index. It normally runs one node;
# during a rollout the autoscaler adds a second so the new qlever pod can sync
# and load there while the old pod keeps serving, then removes the empty node.
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

  # A node upgrade evicts the qlever pod, and qlever is down until the
  # replacement has synced and loaded the index. Only start automatic upgrades
  # overnight: 07:00-11:00 UTC is 2-6am US Eastern in winter (EST) and 3-7am in
  # summer (EDT), since the window is fixed in UTC. GKE requires each window
  # to be at least 4 hours and at least 48 hours of windows in any 32 days,
  # hence a daily window.
  maintenance_policy {
    recurring_window {
      start_time = "2026-01-01T07:00:00Z"
      end_time   = "2026-01-01T11:00:00Z"
      recurrence = "FREQ=DAILY"
    }
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }
}

resource "google_container_node_pool" "qlever" {
  name     = "qlever"
  cluster  = google_container_cluster.qlever.id
  location = var.zone

  # a node only fits one qlever pod; the second node exists only while a
  # rollout's new pod runs alongside the old one
  autoscaling {
    min_node_count = var.min_node_count
    max_node_count = var.max_node_count
  }

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

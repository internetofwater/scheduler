# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

output "cluster_name" {
  description = "Name of the qlever GKE cluster"
  value       = google_container_cluster.qlever.name
}

output "cluster_endpoint" {
  description = "Endpoint of the qlever GKE control plane"
  value       = google_container_cluster.qlever.endpoint
}

output "cluster_ca_certificate" {
  description = "Base64 encoded CA certificate of the qlever GKE control plane"
  value       = google_container_cluster.qlever.master_auth[0].cluster_ca_certificate
}

output "ip_address" {
  description = "Global IP of the qlever load balancer; point the DNS A record of each qlever_url here"
  value       = google_compute_global_address.qlever.address
}

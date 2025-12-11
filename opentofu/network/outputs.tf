# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

output "id" {
  description = "The id of the network"
  value       = google_compute_network.default.id
}

output "network_name" {
  description = "The name of the network"
  value       = google_compute_network.default.name
}

output "static_ip" {
  description = "Static IP address"
  value       = google_compute_address.static_ip.address
}

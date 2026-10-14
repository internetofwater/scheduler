# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

output "static_ip" {
  description = "Static IP address"
  value       = module.network.static_ip
}

output "graph_ip" {
  description = "Static IP address"
  value       = data.google_compute_instance.graphdb.network_interface[0].network_ip
}

# read by scripts/redeploy.sh to know which VM to ssh into
output "vm_name" {
  description = "Name of the scheduler VM"
  value       = var.vm_name
}

output "vm_zone" {
  description = "Zone of the scheduler VM"
  value       = var.zone
}

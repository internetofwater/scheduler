# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# network/main.tf
resource "google_compute_network" "default" {
  name                    = var.vpc_name
  auto_create_subnetworks = true
}

resource "google_compute_global_address" "private_ip_alloc" {
  name          = "private-ip-alloc"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.default.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.default.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_alloc.name]
}

resource "google_compute_address" "static_ip" {
  name   = "static-ip"
  region = var.zone
}

resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow_dagster" {
  name    = "allow-dagster"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = ["3000", "4000"]
  }

  source_ranges = ["0.0.0.0/0"]
}

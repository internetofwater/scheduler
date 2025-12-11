# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# Cloud SQL PostgreSQL Instance
resource "google_sql_database_instance" "postgres" {
  name             = "dagster-postgres-instance"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier            = "db-custom-1-3840"
    disk_autoresize = true
    ip_configuration {
      ipv4_enabled    = false
      private_network = var.network
    }
    database_flags {
      name  = "cloudsql.enable_pg_cron"
      value = "on"
    }
    database_flags {
      name  = "max_connections"
      value = "1000"
    }
  }

  deletion_protection = true
}

resource "google_sql_database" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "dagster" {
  name     = "dagster"
  instance = google_sql_database_instance.postgres.name
  password = var.database_password
}

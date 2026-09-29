# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# Cloud SQL PostgreSQL Instance
resource "google_sql_database_instance" "postgres" {
  name             = "dagster-postgres-instance"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier = "db-custom-2-7680"
    disk_autoresize = true
    ip_configuration {
      ipv4_enabled  = true
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
    user_labels = {
      "id" = "dagster-database"
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

resource "google_cloud_run_v2_job" "postgis_setup" {
  name     = "postgis-extension-setup"
  location = var.region
  template {
    template {
      containers {
        image = "postgres:16"

        env {
          name  = "PGHOST"
          value = "/cloudsql/${google_sql_database_instance.postgres.connection_name}"
        }
        env {
          name  = "PGDATABASE"
          value = google_sql_database.dagster.name
        }
        env {
          name  = "PGUSER"
          value = google_sql_user.dagster.name
        }
        env {
          name  = "PGPASSWORD"
          value = var.database_password
        }

        command = [
          "/bin/bash",
          "-c",
          <<-EOT
            psql -c "CREATE EXTENSION IF NOT EXISTS postgis;"
            psql -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
            psql -c "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"
            psql -c "CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;"
            psql -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
          EOT
        ]

        volume_mounts {
          name = "cloudsql"
          mount_path = "/cloudsql"
        }
      }

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.postgres.connection_name]
        }
      }
    }
  }

  deletion_protection = false
}
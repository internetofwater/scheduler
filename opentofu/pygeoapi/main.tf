# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

resource "google_cloud_run_v2_service" "pygeoapi" {
  name                = "geoconnex-features-pygeoapi"
  location            = var.region
  deletion_protection = false

  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = var.service_account_email
    scaling {
      max_instance_count = 2
      min_instance_count = 1
    }
    volumes {
      name = "config-volume"
      gcs {
        bucket = var.config_bucket
      }
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [var.db_connection_name]
      }
    }
    containers {
      image = "internetofwater/pygeoapi:latest"
      ports {
        container_port = 80
      }

      volume_mounts {
        name       = "config-volume"
        mount_path = "/config"
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      # ideally this would be set here; however, that would make
      # the cloud run service dependent on itself which terraform
      # does not allow; thus this needs to be set manually after deployment

      env {
        name= "PYGEOAPI_URL"
        value = "https://features.geoconnex.us"
      }

      env {
        name  = "PYGEOAPI_CONFIG"
        value = "/config/pygeoapi/pygeoapi.config.yml"
      }
      env {
        name  = "POSTGRES_HOST"
        value = "/cloudsql/${var.db_connection_name}"
      }
      env {
        name  = "POSTGRES_USER"
        value = var.POSTGRES_USER
      }

      env {
        name  = "POSTGRES_PASSWORD"
        value = var.POSTGRES_PASSWORD
      }

      env {
        name  = "POSTGRES_DB"
        value = var.POSTGRES_DB
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        cpu_idle = false
      }
    }
  }

  traffic {
    percent = 100
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_access" {
  location = google_cloud_run_v2_service.pygeoapi.location
  project  = google_cloud_run_v2_service.pygeoapi.project
  name     = google_cloud_run_v2_service.pygeoapi.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_storage_bucket_object" "pygeoapi_config" {
  name   = "pygeoapi/pygeoapi.config.yml"
  bucket = var.config_bucket
  source = "${path.module}/pygeoapi.config.yml"
}


resource "google_storage_bucket_iam_member" "pygeoapi_access" {
  bucket = var.config_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.service_account_email}"
}

# resource "google_cloud_run_domain_mapping" "default" {
#   location = var.region
#   name     = "features.geoconnex.us"

#   metadata {
#     namespace = var.project_id
#   }

#   spec {
#     route_name = google_cloud_run_v2_service.pygeoapi.name
#   }
# }

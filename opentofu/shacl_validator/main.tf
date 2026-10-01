# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# A terraform file to deploy the shacl validation server

resource "google_cloud_run_v2_service" "shacl_service" {
  name = "shacl-validation-server"
  description = "shacl validation server"
  
  # same region as the other services
  location = var.region
  # allow this service to be deleted and redeployed if needed
  deletion_protection = false
  # allow unauthenticated traffic
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      image = "docker.io/internetofwater/nabu:latest"
      ports {
        container_port = 8000
      }
      # start the shacl validation server  
      args = ["shacl", "serve"]
      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        # Determines whether CPU is only allocated during requests
        cpu_idle = true
      }
      env {
        name  = "CATCHMENTS_FILE"
        value = ""
      }
    }
  }
  # allow scale to 0 since we presume there will be times with no validation traffic
  scaling {
    max_instance_count = 1
    min_instance_count = 0
    scaling_mode       = "AUTOMATIC"
  }

  traffic {
    # all traffic should go to the latest version
    percent = 100
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  }
}

# Make the cloud run service public to the internet
resource "google_cloud_run_service_iam_binding" "shacl_service" {
  location = google_cloud_run_v2_service.shacl_service.location
  service  = google_cloud_run_v2_service.shacl_service.name
  role     = "roles/run.invoker"
  members = [
    "allUsers"
  ]
}
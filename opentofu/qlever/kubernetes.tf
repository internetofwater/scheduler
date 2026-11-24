# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# The qlever workload on the GKE cluster from cluster.tf:
#   - the sync-index init container pulls the index from GCS onto the SSD on every pod start
#   - qlever-server serves the index from the SSD and preloads the materialized views
#   - a GKE Ingress with a Google-managed certificate serves each qlever_url over HTTPS

locals {
  labels = { app = "qlever" }
  # qlever user group
  qlever_uid = 999
  qlever_gid = 999
  # timeout passed to qlever-server
  query_timeout_seconds = 300
  index_source          = "gs://${var.s3_bucket}/geoconnex_index"
}

resource "kubernetes_namespace_v1" "qlever" {
  metadata {
    name = "qlever"
  }

  depends_on = [google_container_node_pool.qlever]
}

resource "kubernetes_persistent_volume_v1" "qlever_data" {
  metadata {
    name = "qlever-data"
  }
  spec {
    capacity = {
      storage = "${var.disk_size}Gi"
    }
    access_modes                     = ["ReadWriteOnce"]
    persistent_volume_reclaim_policy = "Retain"
    # not a real StorageClass; it only pairs this volume with the claim below
    # and keeps GKE from dynamically provisioning a different disk
    storage_class_name = "qlever-static"
    persistent_volume_source {
      csi {
        driver        = "pd.csi.storage.gke.io"
        volume_handle = google_compute_disk.qlever_data.id
        fs_type       = "ext4"
      }
    }
  }

  depends_on = [google_container_node_pool.qlever]
}

resource "kubernetes_persistent_volume_claim_v1" "qlever_data" {
  metadata {
    name      = "qlever-data"
    namespace = kubernetes_namespace_v1.qlever.metadata[0].name
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "qlever-static"
    volume_name        = kubernetes_persistent_volume_v1.qlever_data.metadata[0].name
    resources {
      requests = {
        storage = "${var.disk_size}Gi"
      }
    }
  }
}

resource "kubernetes_deployment_v1" "qlever" {
  metadata {
    name      = "qlever"
    namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    labels    = local.labels
  }

  spec {
    replicas = 1
    # syncing and loading a new index may take longer than the default 10 minutes
    progress_deadline_seconds = 3600

    # the persistent disk can only be attached to one node at a time
    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        termination_grace_period_seconds = 30

        # give the qlever user access to the persistent disk
        security_context {
          fs_group               = local.qlever_gid
          fs_group_change_policy = "OnRootMismatch"
        }

        # pull the latest index from GCS before qlever opens it.
        # restarting the deployment serves a new index.
        dynamic "init_container" {
          for_each = var.sync_index_on_start ? [1] : []
          content {
            name    = "sync-index"
            image   = "gcr.io/google.com/cloudsdktool/google-cloud-cli:stable"
            command = ["bash", "-c"]
            args = [<<-EOF
              set -euo pipefail
              src="${local.index_source}"
              mkdir -p /data/index
              echo "Syncing $src to /data/index"
              gcloud storage rsync --recursive --delete-unmatched-destination-objects "$src" /data/index
              chown -R ${local.qlever_uid}:${local.qlever_gid} /data/index
              echo "Index synced"
            EOF
            ]

            env {
              # the image's default config dir is not writable everywhere
              name  = "CLOUDSDK_CONFIG"
              value = "/tmp/gcloud"
            }

            volume_mount {
              name       = "data"
              mount_path = "/data"
            }

            resources {
              requests = {
                cpu    = "2"
                memory = "2Gi"
              }
              limits = {
                memory = "4Gi"
              }
            }
          }
        }

        container {
          name        = "qlever"
          image       = var.qlever_image
          working_dir = "/data/index"
          # bypass the image's entrypoint
          command = ["bash", "-c"]
          args = [<<-EOF
            set -euo pipefail
            shopt -s nullglob
            views=()
            for f in geoconnex.view.*.viewinfo.json; do
              name="$${f#geoconnex.view.}"
              views+=("$${name%.viewinfo.json}")
            done
            if [ "$${#views[@]}" -eq 0 ]; then
              echo "No materialized views found in the index; refusing to start without them" >&2
              exit 1
            fi
            echo "Preloading materialized views: $${views[*]}"
            exec qlever-server -i geoconnex -j 16 -p 8888 -s ${local.query_timeout_seconds}s \
              --memory-max-size ${var.pod_memory_gib - 8}GB \
              --preload-materialized-views "$${views[@]}"
          EOF
          ]

          security_context {
            # uid/gid of the `qlever` user in the qlever image
            run_as_user  = local.qlever_uid
            run_as_group = local.qlever_gid
          }

          port {
            name           = "sparql"
            container_port = 8888
          }

          volume_mount {
            name       = "data"
            mount_path = "/data/index"
            sub_path   = "index"
          }

          resources {
            # request the full limit so the scheduler reserves it for qlever
            requests = {
              cpu    = "4"
              memory = "${var.pod_memory_gib}Gi"
            }
            limits = {
              memory = "${var.pod_memory_gib}Gi"
            }
          }

          # loading the index may take a while; allow 15 minutes
          startup_probe {
            http_get {
              path = "/ping"
              port = 8888
            }
            period_seconds    = 10
            failure_threshold = 90
          }

          readiness_probe {
            http_get {
              path = "/ping"
              port = 8888
            }
            period_seconds = 10
          }

          liveness_probe {
            http_get {
              path = "/ping"
              port = 8888
            }
            period_seconds    = 30
            timeout_seconds   = 5
            failure_threshold = 5
          }
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.qlever_data.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_manifest" "qlever_backend_config" {
  manifest = {
    apiVersion = "cloud.google.com/v1"
    kind       = "BackendConfig"
    metadata = {
      name      = "qlever"
      namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    }
    spec = {
      timeoutSec = local.query_timeout_seconds
      healthCheck = {
        type        = "HTTP"
        requestPath = "/ping"
        port        = 8888
      }
    }
  }
}

resource "kubernetes_manifest" "qlever_frontend_config" {
  manifest = {
    apiVersion = "networking.gke.io/v1beta1"
    kind       = "FrontendConfig"
    metadata = {
      name      = "qlever"
      namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    }
    spec = {
      redirectToHttps = {
        enabled = true
      }
    }
  }
}

resource "kubernetes_manifest" "qlever_certificate" {
  manifest = {
    apiVersion = "networking.gke.io/v1"
    kind       = "ManagedCertificate"
    metadata = {
      name      = "qlever"
      namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    }
    spec = {
      domains = var.qlever_url
    }
  }
}

resource "kubernetes_service_v1" "qlever" {
  metadata {
    name      = "qlever"
    namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    annotations = {
      "cloud.google.com/neg"            = jsonencode({ ingress = true })
      "cloud.google.com/backend-config" = jsonencode({ default = kubernetes_manifest.qlever_backend_config.manifest.metadata.name })
    }
  }

  spec {
    type     = "ClusterIP"
    selector = local.labels

    port {
      name        = "http"
      port        = 80
      target_port = "sparql"
    }
  }
}

resource "kubernetes_ingress_v1" "qlever" {
  metadata {
    name      = "qlever"
    namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    annotations = {
      "kubernetes.io/ingress.class"                 = "gce"
      "kubernetes.io/ingress.global-static-ip-name" = google_compute_global_address.qlever.name
      "networking.gke.io/managed-certificates"      = kubernetes_manifest.qlever_certificate.manifest.metadata.name
      "networking.gke.io/v1beta1.FrontendConfig"    = kubernetes_manifest.qlever_frontend_config.manifest.metadata.name
    }
  }

  spec {
    default_backend {
      service {
        name = kubernetes_service_v1.qlever.metadata[0].name
        port {
          number = 80
        }
      }
    }
  }
}

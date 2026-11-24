# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# The qlever workload on the GKE cluster from cluster.tf:
#   - every qlever pod gets its own new pd-ssd, which is deleted with the pod
#   - the sync-index init container pulls the index from GCS onto that SSD
#   - a rollout starts the new pod on the spare node and only removes the old one once the new one serves
#   - qlever-server serves the index from the SSD and preloads the materialized views
#   - a GKE Ingress with a Google-managed certificate serves each qlever_url over HTTPS

locals {
  labels = { app = "qlever" }
  # qlever user group
  qlever_uid = 999
  qlever_gid = 999
  # timeout passed to qlever-server
  query_timeout_seconds = 300
  # how long the old pod keeps serving after it is taken out of the load balancer
  drain_seconds = 30
  index_source  = "gs://${var.s3_bucket}/geoconnex_index"
}

resource "kubernetes_namespace_v1" "qlever" {
  metadata {
    name = "qlever"
  }

  depends_on = [google_container_node_pool.qlever]
}

# Dynamically provisions an SSD for each qlever pod. Binding waits for the pod
# to be scheduled so the disk is created in the pod's zone
resource "kubernetes_storage_class_v1" "qlever_ssd" {
  metadata {
    name = "qlever-ssd"
  }
  storage_provisioner    = "pd.csi.storage.gke.io"
  reclaim_policy         = "Delete"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  parameters = {
    type = "pd-ssd"
  }

  depends_on = [google_container_node_pool.qlever]
}

resource "kubernetes_deployment_v1" "qlever" {
  # a rollout syncs and loads a whole index before it finishes
  timeouts {
    create = "60m"
    update = "60m"
  }

  metadata {
    name      = "qlever"
    namespace = kubernetes_namespace_v1.qlever.metadata[0].name
    labels    = local.labels
  }

  spec {
    replicas = 1
    # syncing and loading a new index may take longer than the default 10 minutes
    progress_deadline_seconds = 3600

    # start the new pod (on the spare node) and wait until it serves before
    # removing the old one, so a new index rolls out without downtime
    strategy {
      type = "RollingUpdate"
      rolling_update {
        max_surge       = "1"
        max_unavailable = "0"
      }
    }

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        # let queries that are still running on the old pod finish
        termination_grace_period_seconds = local.drain_seconds + local.query_timeout_seconds

        # give the qlever user access to the persistent disk
        security_context {
          fs_group               = local.qlever_gid
          fs_group_change_policy = "OnRootMismatch"
        }

        # pull the latest index from GCS onto the pod's new disk before qlever
        # opens it. restarting the deployment serves a new index.
        init_container {
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

          # keep serving while the load balancer stops sending this pod new queries
          lifecycle {
            pre_stop {
              exec {
                command = ["sleep", "${local.drain_seconds}"]
              }
            }
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

        # a new SSD for each pod, deleted along with the pod
        volume {
          name = "data"
          ephemeral {
            volume_claim_template {
              metadata {
                labels = local.labels
              }
              spec {
                access_modes       = ["ReadWriteOnce"]
                storage_class_name = kubernetes_storage_class_v1.qlever_ssd.metadata[0].name
                resources {
                  requests = {
                    storage = "${var.disk_size}Gi"
                  }
                }
              }
            }
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

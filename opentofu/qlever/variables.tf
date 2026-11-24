# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# qlever/variables.tf

variable "name" {
  description = "Prefix for the qlever GKE cluster and its resources"
  type        = string
}

variable "zone" {
  description = "GCP zone for the zonal GKE cluster and its persistent disk"
  type        = string
}

variable "network_name" {
  description = "Name of the VPC network to attach the cluster to"
  type        = string
}

variable "service_account_email" {
  description = "Service account the GKE nodes run as"
  type        = string
}

variable "qlever_url" {
  description = "Public hostnames qlever is served on; one Google-managed certificate covers all of them"
  type        = list(string)
  default     = ["qlever.internetofwater.app", "graph.geoconnex.us"]
}

variable "machine_type" {
  description = "Machine type for the qlever GKE node"
  type        = string
  default     = "e2-highmem-8"
}

variable "min_node_count" {
  description = "Minimum number of qlever GKE nodes. A node only fits one qlever pod, so 1 serves the index"
  type        = number
  default     = 1
}

variable "max_node_count" {
  description = "Maximum number of qlever GKE nodes. Keep at least 2 so a rollout can start the new pod on a second node for zero downtime"
  type        = number
  default     = 2
}

variable "pod_memory_gib" {
  description = "Memory reserved for and limited to the qlever pod (in GiB). Keep it below the node's allocatable memory."
  type        = number
  default     = 54
}

variable "disk_size" {
  description = "Size of the SSD each qlever pod gets for its copy of the index (in GB)"
  type        = number
  default     = 110
}

variable "qlever_image" {
  description = "QLever container image; keep in sync with IMAGE in assets/Qleverfile"
  type        = string
  default     = "docker.io/adfreiburg/qlever:0.6.0"
}

variable "s3_bucket" {
  description = "GCS bucket the qlever_index export uploads the index to"
  type        = string
}

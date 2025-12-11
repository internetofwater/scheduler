# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# network/variables.tf

# VPC Configuration
variable "vpc_name" {
  description = "Name of the VPC to create or use for the deployment"
  type        = string
  default     = "geoconnex-harvest"
}

variable "project" {
  description = "GCP Project ID for production environment"
  type        = string
}

variable "zone" {
  description = "GCP region for deploying resources"
  type        = string
  default     = "us-central1"
}

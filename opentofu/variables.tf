# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

variable "admin_email" {
  description = "Email of the admin user to grant access to the storage bucket"
  type        = string
  default     = "bwebb@lincolninst.edu"
}

variable "service_account_email" {
  description = "Email of the service user to access to the storage bucket"
  type        = string
  default     = "geoconnex-us@geoconnex-us.iam.gserviceaccount.com"
}

variable "project" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for VM deployment"
  type        = string
  default     = "us-central1-a"
}

variable "s3_bucket" {
  description = "GCP bucket name for storing data"
  type        = string
}

variable "url" {
  description = "URL Host"
  type        = string
  default     = "test.harvest.internetofwater.app"
}

variable "graph_url" {
  description = "Graph URL"
  type        = string
  default     = "test.harvest.graph.internetofwater.app"
}

variable "credentials" {
  description = "Path to GCP credentials"
}

variable "vm_name" {
  description = "Name of the vm to create or use for the deployment"
  type        = string
}

variable "sitemap_url" {
  description = "URL of the sitemap index file"
  type        = string
}

variable "lakefs_endpoint" {
  description = "URL of lakefs endpoint"
  type        = string
}

variable "lakefs_access_key" {
  description = "Lakefs Access Key"
  type        = string
  sensitive   = true
}

variable "lakefs_secret_key" {
  description = "Lakefs Secret Key"
  type        = string
  sensitive   = true
}

variable "zenodo_access_token" {
  description = "Zenodo Access Token"
  type        = string
  sensitive   = true
}

variable "dagster_slack_token" {
  description = "Dagster slack token"
  type        = string
  sensitive   = true
}

variable "database_password" {
  description = "Password for the database user"
  type        = string
  sensitive   = true
}

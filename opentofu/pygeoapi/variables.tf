# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

variable "region" {
  description = "GCP region"
  type        = string
}

variable "POSTGRES_PASSWORD" {
  description = "Password for the database user"
  type        = string
  sensitive   = true
}

variable "POSTGRES_USER" {
  description = "Username for the database user"
  type        = string
}

variable "POSTGRES_DB" {
  description = "Database name"
  type        = string
}

variable "POSTGRES_HOST" {
  description = "Database host"
  type        = string
}

variable "config_bucket" {
  description = "Bucket to store pygeoapi config"
  type        = string
}

variable "service_account_email" {
  description = "Email of the service account to grant access to the storage bucket"
  type        = string
}

variable "db_connection_name" {
  description = "Database connection name"
  type        = string
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}
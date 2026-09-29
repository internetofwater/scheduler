# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

provider "google" {
  project     = var.project
  region      = "us"
  credentials = file(var.credentials) 
}

terraform {
  required_version = ">= 1.5.0"

  # storage location for terraform state
  backend "gcs" {
    # store terraform state in a bucket isolated from other data
    bucket  = var.s3_terraform_state_bucket 
    prefix  = "${var.project}/terraform.tfstate" 
    credentials = file(var.credentials)
  }
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "run.googleapis.com",
    "iam.googleapis.com",
    "cloudbuild.googleapis.com",
    "storage.googleapis.com",
    "servicenetworking.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ])

  project = var.project
  service = each.key

  disable_on_destroy = false
}

module "network" {
  source = "./network"
  project = var.project
}

module "database" {
  source = "./database"
  region  = var.region
  network = module.network.id
  database_password = var.database_password
}

module "storage" {
  source = "./storage"
  service_account_email = var.service_account_email
  project = var.project
  region  = var.region
  s3_bucket  = var.s3_bucket
  s3_metadata_bucket = var.s3_metadata_bucket
  s3_terraform_state_bucket = var.s3_terraform_state_bucket
}

module "graphdb" {
  source            = "github.com/Ontotext-AD/terraform-gcp-graphdb?ref=v0.2.3"
  source_image      = "projects/mpi-graphdb-public/global/images/ontotext-graphdb-10-7-3-202409031355"
  project_id        = var.project
  zone              = var.zone
  boot_disk_size    = 250
  network_interface = module.network.network_name
  allowed_ingress_cidrs = ["10.0.0.0/8", "192.168.0.0/16", "35.191.0.0/16"]
}

module "pygeoapi" {
  source = "./pygeoapi"
  region = var.region
  config_bucket      = module.storage.s3_bucket
  POSTGRES_PASSWORD = var.database_password
  POSTGRES_USER     = module.database.database_user
  POSTGRES_DB       = module.database.database_name
  POSTGRES_HOST     = module.database.private_ip_address
  service_account_email = var.service_account_email
  db_connection_name = module.database.connection_name
  project_id = var.project
}

data "google_compute_instance" "graphdb" {
  name    = "graphdb" 
  project = var.project
  zone    = var.zone

  depends_on = [ module.graphdb ]
}

module "instances" {
  source        = "./instances"
  project       = var.project
  url           = var.url
  name          = var.vm_name
  zone          = var.zone
  sitemap_url   = var.sitemap_url
  service_account_email = var.service_account_email
  graph_url     = "http://${data.google_compute_instance.graphdb.network_interface[0].network_ip}:7200"

  # tokens
  lakefs_endpoint = var.lakefs_endpoint
  lakefs_access_key = var.lakefs_access_key
  lakefs_secret_key = var.lakefs_secret_key
  zenodo_access_token = var.zenodo_access_token
  dagster_slack_token = var.dagster_slack_token
  ghcr_token = var.ghcr_token

  # network configurations
  network_name  = module.network.network_name
  static_ip     = module.network.static_ip

  s3_bucket     = module.storage.s3_bucket
  s3_metadata_bucket = var.s3_metadata_bucket
  s3_region     = module.storage.s3_region
  s3_access_key = module.storage.s3_access_key
  s3_secret_key = module.storage.s3_secret_key

  database_host = module.database.private_ip_address
  database_user = module.database.database_user
  database_name = module.database.database_name
  database_password = var.database_password
}

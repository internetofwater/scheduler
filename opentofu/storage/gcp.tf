# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "harvest_bucket" {
  name          = var.s3_bucket
  location      = var.region
  
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}

resource "google_storage_bucket" "terraform_state_bucket" {
  name = var.s3_terraform_state_bucket
  location = var.region

  storage_class = "STANDARD"
  uniform_bucket_level_access = true 

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "metadata_bucket" {
  name          = var.s3_metadata_bucket
  location      = var.region
  
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", "OPTIONS"]
    response_header = ["*"]
    max_age_seconds = 3600
  }

  versioning {
    enabled = false
  }

}

resource "google_storage_bucket_iam_member" "bucket_access" {
  bucket = google_storage_bucket.harvest_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.service_account_email}"
}

resource "google_storage_bucket_iam_member" "metadata_bucket_access" {
  bucket = google_storage_bucket.metadata_bucket.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

resource "google_storage_bucket_iam_member" "terraform_state_bucket_access" {
  bucket = google_storage_bucket.terraform_state_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.service_account_email}"
}

resource "google_storage_hmac_key" "hmac_key" {
  service_account_email = var.service_account_email
  project               = var.project
}

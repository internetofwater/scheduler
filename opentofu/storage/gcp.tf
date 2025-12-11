# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

resource "google_storage_bucket" "harvest_bucket" {
  name     = var.s3_bucket
  location = var.region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 7 # Move to Coldline after 7 days
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket_iam_member" "bucket_access" {
  bucket = google_storage_bucket.harvest_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.service_account_email}"
}

resource "google_storage_hmac_key" "hmac_key" {
  service_account_email = var.service_account_email
  project               = var.project
}

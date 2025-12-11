# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

output "s3_bucket" {
  value = google_storage_bucket.harvest_bucket.name
}

output "s3_region" {
  value = var.region
}

output "s3_access_key" {
  value     = split("/", google_storage_hmac_key.hmac_key.id)[3]
  sensitive = true
}

output "s3_secret_key" {
  value     = google_storage_hmac_key.hmac_key.secret
  sensitive = true
}

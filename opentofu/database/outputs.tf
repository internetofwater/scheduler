# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

output "instance" {
  description = "The database instance resource"
  value       = google_sql_database_instance.postgres
}

output "instance_name" {
  description = "The name of the database instance"
  value       = google_sql_database_instance.postgres.name
}

output "database_name" {
  description = "The name of the database"
  value       = google_sql_database.dagster.name
}

output "database_user" {
  description = "The database user name"
  value       = google_sql_user.dagster.name
}

output "private_ip_address" {
  description = "The private IP address of the database instance"
  value       = google_sql_database_instance.postgres.private_ip_address
}

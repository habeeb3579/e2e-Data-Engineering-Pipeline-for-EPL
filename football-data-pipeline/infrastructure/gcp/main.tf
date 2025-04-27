# infrastructure/gcp/main.tf
provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
  type        = string
  default     = "football_dataset"
}

variable "bucket_name" {
  description = "The GCS bucket name"
  type        = string
}

# Create GCS bucket for data lake
resource "google_storage_bucket" "data_lake" {
  name     = var.bucket_name
  location = var.region
  force_destroy = true
  
  # Enable versioning
  versioning {
    enabled = true
  }
  
  # Set lifecycle rules
  lifecycle_rule {
    condition {
      age = 90  # 90 days
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  # Enable uniform bucket-level access
  uniform_bucket_level_access = true
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "football_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Football Data"
  description                 = "Dataset containing football data from understat.com"
  location                    = var.region
  default_table_expiration_ms = 31536000000  # 1 year
  
  labels = {
    environment = "production"
    data_source = "understat"
  }
}

# Create service account for data pipeline
resource "google_service_account" "pipeline_service_account" {
  account_id   = "football-pipeline-sa"
  display_name = "Football Data Pipeline Service Account"
  description  = "Service account for the football data pipeline"
}

# Assign BigQuery Admin role to the service account
resource "google_project_iam_binding" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  
  members = [
    "serviceAccount:${google_service_account.pipeline_service_account.email}",
  ]
}

# Assign Storage Admin role to the service account
resource "google_project_iam_binding" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  
  members = [
    "serviceAccount:${google_service_account.pipeline_service_account.email}",
  ]
}

# Create service account key for local usage
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline_service_account.name
}

# Output the service account key (be careful with this in production)
output "service_account_key" {
  value     = google_service_account_key.pipeline_key.private_key
  sensitive = true
}

# Output the bucket name
output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

# Output the dataset ID
output "dataset_id" {
  value = google_bigquery_dataset.football_dataset.dataset_id
}
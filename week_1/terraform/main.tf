terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
  }
}

# Configure the GCP Provider
provider "google" {
  project     = var.project
  region      = var.region
  credentials = var.credentials
}

# Data Buckets
resource "google_storage_bucket" "taxi-bucket" {
  for_each = toset(["${local.raw_data_bucket}-${var.project}", "${local.transformed_data_bucket}-${var.project}"])
  name     = each.key
  location = var.region

  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  force_destroy = true
}

# Data Warehouse
resource "google_bigquery_dataset" "bq_taxi" {
  dataset_id  = var.TAXI_DATA
  project     = var.project
  location    = var.region
  description = "BigQuery dataset that NYC TLC Trip data will be ingested into."
}

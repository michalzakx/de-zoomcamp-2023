locals {
  raw_data_bucket         = "raw-data"
  transformed_data_bucket = "transformed-data"
}

# Project information
variable "project" {
  description = "Your GCP project ID."
  type        = string
}

variable "region" {
  description = "Region for deploying GCP resources. Reference: https://cloud.google.com/about/locations#europe"
  type        = string
  default     = "europe-central2"
}

variable "credentials" {
  description = "Path to the service account key file."
  type        = string
}

# Data Lake Settings
variable "storage_class" {
  description = "Storage class type of the bucket."
  type        = string
  default     = "STANDARD"
}

# Data Warehouse Settings
variable "TAXI_DATA" {
  description = "Name of the NYC TLC Trip dataset."
  type        = string
}

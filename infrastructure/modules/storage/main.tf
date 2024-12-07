variable "cloud_provider" {}
variable "environment" {}
variable "region" {}
variable "project_name" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
}

# AWS S3 Storage
resource "aws_s3_bucket" "data_lake" {
  count  = var.cloud_provider == "aws" ? 1 : 0
  bucket = "${local.resource_prefix}-data-lake"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  count  = var.cloud_provider == "aws" ? 1 : 0
  bucket = aws_s3_bucket.data_lake[0].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Azure Storage Account
resource "azurerm_storage_account" "data_lake" {
  count                    = var.cloud_provider == "azure" ? 1 : 0
  name                     = replace("${local.resource_prefix}datalake", "-", "")
  resource_group_name      = var.resource_group_name
  location                 = var.region
  account_tier            = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled          = true

  tags = {
    environment = var.environment
    project     = var.project_name
  }
}

resource "azurerm_storage_container" "bronze" {
  count                 = var.cloud_provider == "azure" ? 1 : 0
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  count                 = var.cloud_provider == "azure" ? 1 : 0
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  count                 = var.cloud_provider == "azure" ? 1 : 0
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

# GCP Cloud Storage
resource "google_storage_bucket" "data_lake" {
  count         = var.cloud_provider == "gcp" ? 1 : 0
  name          = "${local.resource_prefix}-data-lake"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }

  labels = {
    environment = var.environment
    project     = var.project_name
  }
}

output "storage_location" {
  value = {
    aws   = var.cloud_provider == "aws" ? aws_s3_bucket.data_lake[0].bucket : null
    azure = var.cloud_provider == "azure" ? azurerm_storage_account.data_lake[0].primary_blob_endpoint : null
    gcp   = var.cloud_provider == "gcp" ? google_storage_bucket.data_lake[0].url : null
  }
}

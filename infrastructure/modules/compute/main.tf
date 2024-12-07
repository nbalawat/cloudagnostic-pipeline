variable "cloud_provider" {}
variable "environment" {}
variable "region" {}
variable "project_name" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
}

# AWS EMR Cluster
resource "aws_emr_cluster" "spark_cluster" {
  count         = var.cloud_provider == "aws" ? 1 : 0
  name          = "${local.resource_prefix}-spark-cluster"
  release_label = "emr-6.9.0"
  
  service_role = aws_iam_role.emr_service_role[0].arn

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }

  configurations_json = jsonencode([
    {
      Classification = "spark"
      Properties = {
        "maximizeResourceAllocation" = "true"
      }
    }
  ])

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Azure Databricks
resource "azurerm_databricks_workspace" "spark_cluster" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "${local.resource_prefix}-databricks"
  resource_group_name = var.resource_group_name
  location            = var.region
  sku                = "premium"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# GCP Dataproc
resource "google_dataproc_cluster" "spark_cluster" {
  count  = var.cloud_provider == "gcp" ? 1 : 0
  name   = "${local.resource_prefix}-spark-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type = "n1-standard-4"
    }

    worker_config {
      num_instances = 2
      machine_type = "n1-standard-4"
    }

    software_config {
      image_version = "2.0"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
  }

  labels = {
    environment = var.environment
    project     = var.project_name
  }
}

variable "cloud_provider" {}
variable "environment" {}
variable "region" {}
variable "project_name" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
}

# AWS MSK
resource "aws_msk_cluster" "kafka" {
  count         = var.cloud_provider == "aws" ? 1 : 0
  cluster_name  = "${local.resource_prefix}-kafka"
  kafka_version = "2.8.1"
  
  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.kafka[0].id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.kafka[0].arn
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Azure Event Hubs
resource "azurerm_eventhub_namespace" "kafka" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "${local.resource_prefix}-eventhub"
  location            = var.region
  resource_group_name = var.resource_group_name
  sku                = "Standard"
  capacity           = 1

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "azurerm_eventhub" "pipeline_events" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "pipeline-events"
  namespace_name      = azurerm_eventhub_namespace.kafka[0].name
  resource_group_name = var.resource_group_name
  partition_count     = 2
  message_retention   = 1
}

# GCP Pub/Sub
resource "google_pubsub_topic" "pipeline_events" {
  count = var.cloud_provider == "gcp" ? 1 : 0
  name  = "${local.resource_prefix}-pipeline-events"

  labels = {
    environment = var.environment
    project     = var.project_name
  }
}

resource "google_pubsub_subscription" "pipeline_events" {
  count   = var.cloud_provider == "gcp" ? 1 : 0
  name    = "${local.resource_prefix}-pipeline-events-sub"
  topic   = google_pubsub_topic.pipeline_events[0].name

  ack_deadline_seconds = 20

  labels = {
    environment = var.environment
    project     = var.project_name
  }
}

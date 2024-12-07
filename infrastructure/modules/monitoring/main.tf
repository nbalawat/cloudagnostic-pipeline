variable "cloud_provider" {}
variable "environment" {}
variable "region" {}
variable "project_name" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
}

# AWS CloudWatch
resource "aws_cloudwatch_dashboard" "pipeline" {
  count          = var.cloud_provider == "aws" ? 1 : 0
  dashboard_name = "${local.resource_prefix}-pipeline-metrics"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["Pipeline", "ProcessingTime", "Layer", "Bronze"],
            ["Pipeline", "ProcessingTime", "Layer", "Silver"],
            ["Pipeline", "ProcessingTime", "Layer", "Gold"]
          ]
          period = 300
          stat   = "Average"
          region = var.region
          title  = "Processing Time by Layer"
        }
      },
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["Pipeline", "QualityCheckFailures"],
            ["Pipeline", "DataVolume"]
          ]
          period = 300
          stat   = "Sum"
          region = var.region
          title  = "Pipeline Health Metrics"
        }
      }
    ]
  })
}

# Azure Monitor
resource "azurerm_monitor_action_group" "pipeline" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "${local.resource_prefix}-alerts"
  resource_group_name = var.resource_group_name
  short_name          = "pipeline"

  email_receiver {
    name          = "pipeline-team"
    email_address = "pipeline-team@example.com"
  }
}

resource "azurerm_monitor_metric_alert" "processing_time" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "${local.resource_prefix}-processing-time"
  resource_group_name = var.resource_group_name
  scopes              = [var.databricks_workspace_id]
  description         = "Alert when processing time exceeds threshold"

  criteria {
    metric_namespace = "Microsoft.Databricks/workspaces"
    metric_name      = "ProcessingTime"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 3600
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline[0].id
  }
}

# GCP Monitoring
resource "google_monitoring_dashboard" "pipeline" {
  count          = var.cloud_provider == "gcp" ? 1 : 0
  dashboard_json = jsonencode({
    displayName = "${local.resource_prefix}-pipeline-metrics"
    gridLayout = {
      widgets = [
        {
          title = "Processing Time by Layer"
          xyChart = {
            dataSets = [
              {
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"custom.googleapis.com/pipeline/processing_time\""
                    aggregation = {
                      alignmentPeriod = "300s"
                      crossSeriesReducer = "REDUCE_MEAN"
                      groupByFields = ["resource.label.layer"]
                    }
                  }
                }
              }
            ]
          }
        },
        {
          title = "Data Quality Metrics"
          xyChart = {
            dataSets = [
              {
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"custom.googleapis.com/pipeline/quality_checks\""
                    aggregation = {
                      alignmentPeriod = "300s"
                      perSeriesAligner = "ALIGN_SUM"
                    }
                  }
                }
              }
            ]
          }
        }
      ]
    }
  })
}

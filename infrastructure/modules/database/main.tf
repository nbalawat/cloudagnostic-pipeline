variable "cloud_provider" {}
variable "environment" {}
variable "region" {}
variable "project_name" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
}

# AWS RDS PostgreSQL
resource "aws_db_instance" "postgres" {
  count               = var.cloud_provider == "aws" ? 1 : 0
  identifier          = "${local.resource_prefix}-postgres"
  engine             = "postgres"
  engine_version     = "14"
  instance_class     = "db.t3.medium"
  allocated_storage  = 20
  
  db_name            = "pipeline_config"
  username          = "admin"
  password          = random_password.db_password[0].result

  backup_retention_period = 7
  multi_az               = var.environment == "prod" ? true : false
  skip_final_snapshot    = true

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Azure PostgreSQL
resource "azurerm_postgresql_server" "postgres" {
  count               = var.cloud_provider == "azure" ? 1 : 0
  name                = "${local.resource_prefix}-postgres"
  location            = var.region
  resource_group_name = var.resource_group_name

  administrator_login          = "admin"
  administrator_login_password = random_password.db_password[0].result

  sku_name   = "GP_Gen5_2"
  version    = "11"
  storage_mb = 20480

  backup_retention_days        = 7
  geo_redundant_backup_enabled = var.environment == "prod" ? true : false
  auto_grow_enabled           = true
  
  ssl_enforcement_enabled     = true

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# GCP Cloud SQL PostgreSQL
resource "google_sql_database_instance" "postgres" {
  count           = var.cloud_provider == "gcp" ? 1 : 0
  name            = "${local.resource_prefix}-postgres"
  database_version = "POSTGRES_14"
  region          = var.region

  settings {
    tier = "db-f1-micro"
    
    backup_configuration {
      enabled = true
      start_time = "02:00"
    }

    availability_type = var.environment == "prod" ? "REGIONAL" : "ZONAL"
  }

  deletion_protection = false
}

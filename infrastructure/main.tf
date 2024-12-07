terraform {
  required_version = ">= 1.0.0"
  
  backend "local" {
    path = "terraform.tfstate"
  }
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

variable "cloud_provider" {
  description = "Cloud provider to deploy to (aws, azure, gcp)"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "region" {
  description = "Region to deploy resources"
  type        = string
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "data-pipeline"
}

module "storage" {
  source         = "./modules/storage"
  cloud_provider = var.cloud_provider
  environment    = var.environment
  region         = var.region
  project_name   = var.project_name
}

module "compute" {
  source         = "./modules/compute"
  cloud_provider = var.cloud_provider
  environment    = var.environment
  region         = var.region
  project_name   = var.project_name
}

module "database" {
  source         = "./modules/database"
  cloud_provider = var.cloud_provider
  environment    = var.environment
  region         = var.region
  project_name   = var.project_name
}

module "kafka" {
  source         = "./modules/kafka"
  cloud_provider = var.cloud_provider
  environment    = var.environment
  region         = var.region
  project_name   = var.project_name
}

module "monitoring" {
  source         = "./modules/monitoring"
  cloud_provider = var.cloud_provider
  environment    = var.environment
  region         = var.region
  project_name   = var.project_name
}

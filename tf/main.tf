# This file is used to deploy the infrastructure for the Tashio project.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.2.0"
}

# Provider configuration for dev environment
provider "aws" {
  alias  = "dev"
  region = "eu-west-1"
  assume_role {
    role_arn = "arn:aws:iam::${var.dev_account_id}:role/TerraformRole"
  }
}

# Provider configuration for prod environment
provider "aws" {
  alias  = "prod"
  region = "eu-west-1"
  assume_role {
    role_arn = "arn:aws:iam::${var.prod_account_id}:role/TerraformRole"
  }
}

# Timestream database for dev environment
resource "aws_timestreamwrite_database" "dev" {
  provider = aws.dev
  database_name = "tashio_timestream_dev"

  tags = {
    Environment = "dev"
    Project     = "tashio"
  }
}

# Timestream table for dev environment
resource "aws_timestreamwrite_table" "dev" {
  provider = aws.dev
  database_name = aws_timestreamwrite_database.dev.database_name
  table_name    = "tashio_metrics_dev"
  
  retention_properties {
    magnetic_store_retention_period_in_days = 7
    memory_store_retention_period_in_hours  = 24
  }

  tags = {
    Environment = "dev"
    Project     = "tashio"
  }
}

# Timestream database for prod environment
resource "aws_timestreamwrite_database" "prod" {
  provider = aws.prod
  database_name = "tashio_timestream_prod"

  tags = {
    Environment = "prod"
    Project     = "tashio"
  }
}

# Timestream table for prod environment
resource "aws_timestreamwrite_table" "prod" {
  provider = aws.prod
  database_name = aws_timestreamwrite_database.prod.database_name
  table_name    = "tashio_metrics_prod"
  
  retention_properties {
    magnetic_store_retention_period_in_days = 30
    memory_store_retention_period_in_hours  = 24
  }

  tags = {
    Environment = "prod"
    Project     = "tashio"
  }
}


# 

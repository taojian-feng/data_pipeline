terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }

  # Configure per environment: terraform init -backend-config=env/dev.s3.tfbackend
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "gridlake"
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "github.com/taojian-feng/data_pipeline"
    }
  }
}

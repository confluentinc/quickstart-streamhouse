terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.17.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0"
    }
  }
}

# The operator's ambient AWS credentials (profile, SSO, or environment), in core's region.
provider "aws" {
  region = local.core.cloud_region
}

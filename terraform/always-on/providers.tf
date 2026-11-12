terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.73.0" # aws_lambda_permission.invoked_via_function_url
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

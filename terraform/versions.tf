terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Remote state in the bucket created by terraform/bootstrap.
  #
  # Backend blocks cannot interpolate variables, so the bucket name is literal.
  # It is derived from the project id: "<project_id>-tfstate".
  backend "gcs" {
    bucket = "aide-playground-tfstate"
    prefix = "platform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

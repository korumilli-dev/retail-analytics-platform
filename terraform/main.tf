terraform {

  required_providers {

    google = {

      source  = "hashicorp/google"

      version = "~> 5.0"

    }

  }

}

provider "google" {

  project = "top-vial-497707-r1"

  region  = "asia-south1"

}


resource "google_storage_bucket" "terraform_demo_bucket" {

  name     = "top-vial-497707-r1-terraform-demo"

  location = "ASIA-SOUTH1"

  force_destroy = true

  uniform_bucket_level_access = true

}



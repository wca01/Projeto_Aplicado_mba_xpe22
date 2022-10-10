# criand backend do terraform
terraform {
  backend "s3" {
    bucket = "terraform-state-mba2022"
    key    = "projeto-mba22/terraform.tfstate"
    region = "us-east-2"
  }
}
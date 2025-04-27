# infrastructure/gcp/terraform.tfvars.example
# Copy this file to terraform.tfvars and update with your values
project_id  = "football-data-project"
region      = "us-central1"
dataset_id  = "football_dataset"
bucket_name = "football-data-lake-unique-name"  # Must be globally unique
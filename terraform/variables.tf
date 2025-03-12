variable "google_credentials_path" {
  description = "Google crenentials to access service account"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "terraform-demo-453301"
}

variable "region" {
  description = "Region"
  default     = "us-central1"

}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My Big Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-453301-terraform-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

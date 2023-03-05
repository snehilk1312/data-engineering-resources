provider "google" {
    project     = "axa-world-123456"
    region      = "asia-south1"
}

resource "google_storage_bucket" "devbucket" {
    name = "devbucket"
    location = "asia-south1"
    storage_class = "STANDARD" # BALANCED
    force_destroy = true
    uniform_bucket_level_access = true
}
  
resource "google_bigquery_dataset" "bqdataset" {
    dataset_id = "bqdataset"
    location = "asia-south1"
    description = "This is a test dataset"
    labels = {
        env = "dev"
    }
    project = "axa-world-123456"
}
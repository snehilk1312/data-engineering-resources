provider "google" {
  project = "eco-world-378913"
  region  = "us-central1"
  zone    = "us-central1-a"
}

resource "google_compute_network" "prod-vpc" {
  name = "production-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "prod-subnet" {
  name = "production-subnetwork"
  ip_cidr_range = "10.3.0.0/16"
  network = google_compute_network.prod-vpc.id
}
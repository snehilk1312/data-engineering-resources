# create google compute instance using terraform
provider "google" {
  project = "eco-world-378913"
  region  = "us-central1"
  zone    = "us-central1-a"
}
  
resource "google_compute_instance" "default" {
  name         = "terraform-instance"
  machine_type = "f1-micro"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = "default"

    access_config {
      # Ephemeral IP
    }
  }
}
# create the main entertainment server on gcp, using terraform and same as from console
provider "google" {
  project = "home-server-11231"
  region  = "asia-south1-c"
}

data "google_compute_image" "ubuntu-lts" {
 project = "ubuntu-os-cloud"
 name = "ubuntu-2204-jammy-v20230214"
}

resource "google_compute_instance" "snehil-homeserver" {
  name         = "snehil-homeserver"
  machine_type = "e2-medium"
  zone         = "asia-south1-c"
  allow_stopping_for_update = true

  boot_disk {
    source = google_compute_disk.snehil-disk.name
  }

  network_interface {
    network = "default"

    access_config {
    }
  }

  metadata = {
    ssh-keys = "snehil:${file("snehil.pub")}"
  }
  metadata_startup_script = "${file("startup.sh")}"
}


resource "google_compute_disk" "snehil-disk" {
  name = "home-server-disk"
  size = 60
  zone = "asia-south1-c"
  image = data.google_compute_image.ubuntu-lts.self_link
  type = "pd-balanced"
}

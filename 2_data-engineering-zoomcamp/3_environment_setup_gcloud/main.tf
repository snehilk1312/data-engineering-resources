# create google compute instance using terraform
provider "google" {
  project = "axa-world-123456"
  region  = "asia-south2"
  zone    = "asia-south2-a"
}

data "google_compute_image" "ubuntu-lts" {
 project = "ubuntu-os-cloud"
 name = "ubuntu-2204-jammy-v20230214"
}

resource "google_compute_instance" "devserver-dez" {
  name         = "snehil-devserver"
  machine_type = "custom-2-4096"
  zone         = "asia-south2-a"
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
  #metadata_startup_script = "${file("startup.sh")}"

  connection {
       type        = "ssh"
       user        = "snehil"
       private_key = file("snehil")
       host        = self.network_interface[0].access_config[0].nat_ip
    }

  provisioner "file" {
    source = "docker-compose.yml"
    destination = "/home/snehil/docker-compose.yml"
  }

  provisioner "file" {
    source = "gcloud.json"
    destination = "/home/snehil/gcloud.json"
  }

  provisioner "file" {
    source = "startup.sh"
    destination = "/home/snehil/startup.sh"
  }

  provisioner "remote-exec" {
    connection {
      host        = self.network_interface[0].access_config[0].nat_ip
      type        = "ssh"
      user        = "snehil"
      timeout     = "1500s"
      private_key = file("snehil")
    }
    inline = [
      "bash /home/snehil/startup.sh"
    ]
  }


  depends_on = [
    google_compute_disk.snehil-disk
  ]
}

output "public_ip" {
  value = google_compute_instance.devserver-dez.network_interface[0].access_config[0].nat_ip
}

resource "google_compute_disk" "snehil-disk" {
  name = "dev-server-disk"
  size = 30
  zone = "asia-south2-a"
  image = data.google_compute_image.ubuntu-lts.self_link
  type = "pd-standard"

}

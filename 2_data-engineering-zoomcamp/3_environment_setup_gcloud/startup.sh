#!/bin/bash
sudo apt update && sudo apt install  -y neofetch unzip zip 

# Install Docker and Docker Compose
# Docker
sudo apt install docker.io -y
sudo systemctl start docker && sudo systemctl enable docker
sudo usermod -aG docker $USER

# Docker Compose
mkdir -p $HOME/.docker/cli-plugins/
curl -SL https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-linux-x86_64 -o $HOME/.docker/cli-plugins/docker-compose
chmod +x $HOME/.docker/cli-plugins/docker-compose

# Docker Compose - root
sudo mkdir -p /root/.docker/cli-plugins/
sudo curl -SL https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-linux-x86_64 -o /root/.docker/cli-plugins/docker-compose
sudo chmod +x /root/.docker/cli-plugins/docker-compose

# Install Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh -O $HOME/anaconda.sh
bash $HOME/anaconda.sh -b -p $HOME/anaconda
eval "$($HOME/anaconda/bin/conda shell.bash hook)"
conda init

# Install Terraform
wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform -y

# Install gcloud cli
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-419.0.0-linux-x86_64.tar.gz
tar -xf $HOME/google-cloud-cli-419.0.0-linux-x86.tar.gz
CLOUDSDK_CORE_DISABLE_PROMPTS=1 $HOME/google-cloud-sdk/install.sh
$HOME/google-cloud-sdk/bin/gcloud init
gcloud auth activate-service-account --key-file=$HOME/gcloud.json
gcloud config set project axa-world-123456

# Run docker compose up -d
# newgrp docker
sudo docker compose up -d
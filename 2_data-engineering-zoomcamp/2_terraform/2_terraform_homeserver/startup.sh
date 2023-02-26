#!/bin/bash
sudo apt update && sudo apt install neofetch unzip zip -y
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker snehil

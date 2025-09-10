#!/bin/bash
set -ex

sudo apt-get update -y
sudo apt-get install -y docker.io git
sudo systemctl enable --now docker
# sudo yum update -y
# sudo yum install git docker -y
# sudo service docker start

sudo chmod 666 /var/run/docker.sock
sudo usermod -a -G docker ubuntu

# Install Docker Compose v2
sudo curl -L \
  https://github.com/docker/compose/releases/download/v2.32.0/docker-compose-$(uname -s)-$(uname -m) \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Prepare Airflow project dirs
mkdir -p ~/app/{dags,logs,plugins,config}
cd ~/app
echo "AIRFLOW_UID=$(id -u)" > .env

# Verify installs
docker --version
docker compose version
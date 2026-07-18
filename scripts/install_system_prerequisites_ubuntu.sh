#!/usr/bin/env bash
set -euo pipefail

# Audited for Ubuntu 24.04 using Docker's official apt-repository procedure.
# Run this script directly as the normal user; it invokes sudo where required.

if [[ ! -r /etc/os-release ]]; then
  echo "Cannot identify the operating system" >&2
  exit 1
fi

. /etc/os-release
if [[ "${ID:-}" != "ubuntu" ]]; then
  echo "This installer supports Ubuntu only; detected ${ID:-unknown}" >&2
  exit 1
fi

sudo apt-get update
sudo apt-get install -y ca-certificates curl python3-venv

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

architecture="$(dpkg --print-architecture)"
codename="${UBUNTU_CODENAME:-$VERSION_CODENAME}"
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${codename}
Components: stable
Architectures: ${architecture}
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt-get update
sudo apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker "${SUDO_USER:-$USER}"

sudo docker run --rm hello-world
sudo docker compose version

echo
echo "System prerequisites installed. Log out and back in so docker-group"
echo "membership takes effect, then run scripts/bootstrap_development.sh."

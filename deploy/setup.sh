#!/bin/bash
set -e

echo "========================================="
echo "   Zero-Rated Download Manager Setup   "
echo "========================================="
echo ""
echo "This wizard will install and configure:"
echo " - Go 1.22"
echo " - Caddy (for TLS/HTTPS)"
echo " - rclone (for GDrive support)"
echo " - The backend Go binary as a systemd service"
echo ""

PUBLIC_IP=$(curl -4 -s ifconfig.me)
BACKEND_DOMAIN="${PUBLIC_IP}.nip.io"
GH_PAGES_URL="https://lazyseahorse.github.io"
GH_USER="lazyseahorse"

echo "Auto-detected Public IP: $PUBLIC_IP"
echo "Using Backend Domain: $BACKEND_DOMAIN"
echo "Using GitHub User: $GH_USER"

if [ -f /etc/zerorated/users.json ]; then
  echo "Found existing users.json, preserving your existing API Key..."
  API_KEY="[Hidden - Saved from previous install]"
else
  API_KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
fi

echo ""
echo "[1/6] Setting up system user and directories..."
sudo useradd -r -s /sbin/nologin zerorated || true
sudo mkdir -p /var/lib/zerorated/scratch /opt/zerorated /etc/zerorated
sudo chown -R zerorated:zerorated /var/lib/zerorated

echo "[2/6] Generating users.json..."
if [ ! -f /etc/zerorated/users.json ]; then
  cat <<EOF | sudo tee /etc/zerorated/users.json > /dev/null
[
  {"api_key": "$API_KEY", "name": "admin", "is_owner": true}
]
EOF
else
  echo "users.json already exists, skipping generation."
fi

echo "[3/6] Installing Caddy..."
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg --yes
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install -y caddy rclone

echo "[4/6] Configuring Caddy..."
cat <<EOF | sudo tee /etc/caddy/Caddyfile > /dev/null
$BACKEND_DOMAIN {
    reverse_proxy localhost:8080
}
EOF
sudo systemctl restart caddy

echo "[5/6] Installing Go & Building Backend..."
ARCH=$(uname -m)
case "$ARCH" in
  x86_64)
    GO_ARCH="amd64"
    ;;
  aarch64|arm64)
    GO_ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

if [ -x "/usr/local/go/bin/go" ]; then
  echo "Go is already installed, skipping download."
  export PATH=$PATH:/usr/local/go/bin
else
  GO_VERSION="1.22.2"
  curl -sL https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz | sudo tar -C /usr/local -xz
  export PATH=$PATH:/usr/local/go/bin
fi

cd /tmp
rm -rf all-zeroes
git clone https://github.com/$GH_USER/all-zeroes.git
cd all-zeroes/backend
/usr/local/go/bin/go mod tidy
GOOS=linux GOARCH=${GO_ARCH} CGO_ENABLED=0 /usr/local/go/bin/go build -o zerorated ./cmd/server
sudo mv zerorated /opt/zerorated/server
sudo chmod +x /opt/zerorated/server

echo "[6/6] Setting up systemd service..."
cat <<EOF | sudo tee /etc/systemd/system/zerorated.service > /dev/null
[Unit]
Description=Zero-rated download manager
After=network.target

[Service]
Type=simple
User=zerorated
ExecStart=/opt/zerorated/server
Restart=always
RestartSec=5
Environment=DB_PATH=/var/lib/zerorated/state.db
Environment=SCRATCH_DIR=/var/lib/zerorated/scratch
Environment=LISTEN_ADDR=127.0.0.1:8080
Environment=ALLOWED_ORIGINS=$GH_PAGES_URL
Environment=USERS_FILE=/etc/zerorated/users.json
Environment=RCLONE_REMOTE=gdrive:zerorated
Environment=MAX_CONCURRENT_ACQUIRES=4
Environment=CHUNK_SIZE_BYTES=1610612736

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now zerorated
sudo systemctl restart zerorated

echo "========================================="
echo "            Setup Complete!              "
echo "========================================="
echo "Backend URL:   https://$BACKEND_DOMAIN"
echo "Admin API Key: $API_KEY"
echo ""
echo "IMPORTANT: Save the API Key! You will need to enter it in the PWA Settings tab."
echo ""
echo "NOTE: If you plan to use the GDrive tier, you must now run:"
echo "      'rclone config' and create a remote named 'gdrive'."
echo "========================================="

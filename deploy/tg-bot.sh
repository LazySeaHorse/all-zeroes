#!/bin/bash
set -e

echo "========================================="
echo "   Zero-Rated Telegram Bot Setup        "
echo "========================================="
echo ""
echo "Prerequisites: the main backend (setup.sh) must already be installed."
echo "You will need:"
echo " - A Telegram bot token (from @BotFather)"
echo " - Your Telegram chat ID (send /start to @userinfobot to find it)"
echo ""

GH_USER="lazyseahorse"

# ---- collect inputs --------------------------------------------------------

read -p "Telegram bot token: " BOT_TOKEN
read -p "Your Telegram chat ID: " CHAT_ID

# Auto-read API key from existing users.json
API_KEY=$(python3 -c "import json; print(json.load(open('/etc/zerorated/users.json'))[0]['api_key'])" 2>/dev/null || true)
if [ -z "$API_KEY" ]; then
  echo "ERROR: Could not read API key from /etc/zerorated/users.json."
  echo "Make sure the main backend is installed first (setup.sh)."
  exit 1
fi
echo "Backend API key auto-detected from users.json."

echo ""
echo "[1/3] Building the bot binary..."
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

export PATH=$PATH:/usr/local/go/bin

cd /tmp
rm -rf all-zeroes-tgbot
git clone https://github.com/$GH_USER/all-zeroes.git all-zeroes-tgbot
cd all-zeroes-tgbot/backend/cmd/tgbot
/usr/local/go/bin/go mod tidy
GOOS=linux GOARCH=${GO_ARCH} CGO_ENABLED=0 /usr/local/go/bin/go build -o tgbot .
sudo mv tgbot /opt/zerorated/tgbot
sudo chmod +x /opt/zerorated/tgbot

echo "[2/3] Writing env file..."
cat <<EOF | sudo tee /etc/zerorated/tgbot.env > /dev/null
TELEGRAM_BOT_TOKEN=$BOT_TOKEN
TELEGRAM_ALLOWED_CHAT_IDS=$CHAT_ID
BACKEND_API_KEY=$API_KEY
EOF
sudo chmod 600 /etc/zerorated/tgbot.env
sudo chown zerorated:zerorated /etc/zerorated/tgbot.env

echo "[3/3] Installing systemd service..."
cat <<EOF | sudo tee /etc/systemd/system/zerorated-tgbot.service > /dev/null
[Unit]
Description=Zero-rated Telegram bot
After=network.target zerorated.service

[Service]
Type=simple
User=zerorated
EnvironmentFile=/etc/zerorated/tgbot.env
ExecStart=/opt/zerorated/tgbot
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now zerorated-tgbot

echo "========================================="
echo "         Telegram Bot is running!        "
echo "========================================="
echo ""
echo "First, configure your Nextcloud share in the bot:"
echo "  /setnc <webdav-url> <share-token>"
echo ""
echo "Then send a URL or file to start a job."
echo "Commands: /list · /nc · /setnc <url> <token> · /cancel <id> · /deliver <id>"
echo ""
echo "To check logs: sudo journalctl -u zerorated-tgbot -f"
echo "========================================="

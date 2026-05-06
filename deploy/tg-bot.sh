#!/bin/bash
set -e

echo "========================================="
echo "   Zero-Rated Telegram Bot Setup        "
echo "========================================="
echo ""
echo "Prerequisites: the main backend (setup.sh) must already be installed."
echo "You will need:"
echo " - A Telegram bot token (from @BotFather)"
echo " - Your Telegram chat ID (send a message to @userinfobot to find it)"
echo " - Your Nextcloud share URL and token (same as the PWA settings)"
echo ""

GH_USER="lazyseahorse"

# ---- collect inputs --------------------------------------------------------

read -p "Telegram bot token: " BOT_TOKEN
read -p "Your Telegram chat ID: " CHAT_ID
read -p "Nextcloud share URL (e.g. https://cloud.example.com/public.php/webdav): " NC_URL
read -p "Nextcloud share token: " NC_TOKEN

# Auto-detect backend API key from existing users.json
if [ -f /etc/zerorated/users.json ]; then
  API_KEY=$(python3 -c "import json,sys; data=json.load(open('/etc/zerorated/users.json')); print(data[0]['api_key'])" 2>/dev/null || true)
fi
if [ -z "$API_KEY" ]; then
  read -p "Backend API key (from /etc/zerorated/users.json): " API_KEY
fi

BACKEND_URL="http://localhost:8080"

echo ""
echo "[1/3] Building the bot binary..."
export PATH=$PATH:/usr/local/go/bin

cd /tmp
rm -rf all-zeroes-tgbot
git clone https://github.com/$GH_USER/all-zeroes.git all-zeroes-tgbot
cd all-zeroes-tgbot/backend/cmd/tgbot
/usr/local/go/bin/go mod tidy
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 /usr/local/go/bin/go build -o tgbot .
sudo mv tgbot /opt/zerorated/tgbot
sudo chmod +x /opt/zerorated/tgbot

echo "[2/3] Writing env file..."
cat <<EOF | sudo tee /etc/zerorated/tgbot.env > /dev/null
TELEGRAM_BOT_TOKEN=$BOT_TOKEN
TELEGRAM_ALLOWED_CHAT_IDS=$CHAT_ID
BACKEND_URL=$BACKEND_URL
BACKEND_API_KEY=$API_KEY
NEXTCLOUD_URL=$NC_URL
NEXTCLOUD_TOKEN=$NC_TOKEN
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
echo "Send your bot a URL or a file to get started."
echo "Commands: /list · /cancel <id> · /deliver <id>"
echo ""
echo "To check logs: sudo journalctl -u zerorated-tgbot -f"
echo "========================================="

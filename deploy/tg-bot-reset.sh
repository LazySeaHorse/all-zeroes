#!/bin/bash
set -e

echo "========================================="
echo "   Zero-Rated Telegram Bot Reset        "
echo "========================================="
echo ""
echo "WARNING: This will stop and remove the Telegram bot service,"
echo "its binary, and the env file (bot token, chat ID, credentials)."
echo ""
read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "[1/3] Stopping the service..."
    sudo systemctl stop zerorated-tgbot || true
    sudo systemctl disable zerorated-tgbot || true

    echo "[2/3] Removing binary and env file..."
    sudo rm -f /opt/zerorated/tgbot
    sudo rm -f /etc/zerorated/tgbot.env
    sudo rm -f /etc/systemd/system/zerorated-tgbot.service

    echo "[3/3] Reloading systemd..."
    sudo systemctl daemon-reload

    echo "========================================="
    echo " Reset complete. Run tg-bot.sh to reinstall."
    echo "========================================="
else
    echo "Aborted. No changes were made."
fi

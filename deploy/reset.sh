#!/bin/bash
set -e

echo "========================================="
echo "   Zero-Rated Download Manager Reset     "
echo "========================================="
echo ""
echo "WARNING: This will completely wipe your database, job history,"
echo "scratch files, API key, and remove the installed service."
echo ""
read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "[1/3] Stopping the services..."
    sudo systemctl stop zerorated || true
    sudo systemctl disable zerorated || true
    sudo systemctl stop caddy || true
    sudo systemctl disable caddy || true

    echo "[2/3] Removing database, scratch directory, binary, and configs..."
    sudo rm -rf /var/lib/zerorated
    sudo rm -rf /opt/zerorated
    sudo rm -rf /etc/zerorated
    
    # Also remove the systemd unit file
    sudo rm -f /etc/systemd/system/zerorated.service

    echo "[3/3] Removing system user and reloading systemd..."
    sudo userdel zerorated || true
    sudo systemctl daemon-reload

    echo "========================================="
    echo " Reset complete! The system is wiped."
    echo " You can now run setup.sh for a fresh start."
    echo "========================================="
else
    echo "Aborted. No changes were made."
fi

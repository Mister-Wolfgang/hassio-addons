#!/bin/bash
set -e

INSTALL_DIR=/opt/claude-bridge

echo "Installation Claude Bridge dans $INSTALL_DIR..."
sudo mkdir -p "$INSTALL_DIR"
sudo cp bridge.py requirements.txt "$INSTALL_DIR/"

python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --no-cache-dir -r "$INSTALL_DIR/requirements.txt"

sudo cp claude-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable claude-bridge
sudo systemctl restart claude-bridge

echo "Bridge démarré sur le port 9099"
echo "Test : curl http://localhost:9099/health"

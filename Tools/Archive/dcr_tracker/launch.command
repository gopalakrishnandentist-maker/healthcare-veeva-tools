#!/bin/bash
# DCR Tracker Tool — Double-click to launch
# This file opens Terminal and starts the app automatically

source ~/.bash_profile 2>/dev/null || source ~/.zshrc 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting DCR Tracker Tool on http://localhost:8510 ..."
sleep 1 && open "http://localhost:8510" &
/opt/anaconda3/bin/streamlit run dcr_app.py --server.port 8510 --server.headless true

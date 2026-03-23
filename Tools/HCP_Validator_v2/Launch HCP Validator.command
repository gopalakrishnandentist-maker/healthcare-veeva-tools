#!/bin/bash
# ─────────────────────────────────────────────────────────
#  HCP Data Validator v3.5 — Streamlit Launcher
#  Double-click this file to start the application
# ─────────────────────────────────────────────────────────

# Navigate to the script's own directory
cd "$(dirname "$0")"

echo ""
echo "  ╔═══════════════════════════════════════════════╗"
echo "  ║   HCP Data Validator v3.5                     ║"
echo "  ║   OpenData India  |  Veeva Systems            ║"
echo "  ║   Streamlit Edition                           ║"
echo "  ╚═══════════════════════════════════════════════╝"
echo ""

# Check if Python is available
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif command -v python &> /dev/null; then
    PYTHON=python
else
    echo "  ERROR: Python is not installed."
    echo "  Please install Python from https://www.python.org/downloads/"
    echo ""
    echo "  Press any key to close..."
    read -n 1
    exit 1
fi

echo "  Using: $($PYTHON --version)"
echo ""

# Check and install dependencies if needed
echo "  Checking dependencies..."
$PYTHON -c "import pandas, openpyxl, streamlit, plotly, requests, bs4" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "  Installing required packages (first time only)..."
    $PYTHON -m pip install pandas openpyxl streamlit plotly requests beautifulsoup4 --quiet
    echo "  Dependencies installed."
fi

echo ""
echo "  Launching HCP Data Validator in your browser..."
echo "  (Close this terminal window to stop the app)"
echo ""

PORT=8504

# If already running, just open the browser
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

# Open browser after a short delay
(sleep 2 && open "http://localhost:$PORT") &

# Launch Streamlit
$PYTHON -m streamlit run hcp_data_validator.py \
    --server.headless true \
    --server.port $PORT \
    --browser.gatherUsageStats false \
    --server.address localhost

echo ""
echo "  Application closed."
echo "  Press any key to close this window..."
read -n 1

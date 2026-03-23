#!/bin/bash
# ─────────────────────────────────────────────────────
#  HCP / HCO Duplicate Identification Tool — Mac Launcher
#  Double-click this file to start the app
# ─────────────────────────────────────────────────────

# Get the directory where this script lives
DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "  ╔═══════════════════════════════════════════════╗"
echo "  ║  HCP / HCO Duplicate Identification Tool v3.0 ║"
echo "  ║  Starting...                                  ║"
echo "  ╚═══════════════════════════════════════════════╝"
echo ""

# Stay in the script's own directory (where all modules live)
cd "$DIR"

# Activate conda base environment (matches your setup)
source /opt/anaconda3/etc/profile.d/conda.sh 2>/dev/null
conda activate base 2>/dev/null

# Check dependencies
python -c "import streamlit" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "  Installing required packages..."
    pip install streamlit altair rapidfuzz pyyaml openpyxl tqdm
    echo ""
fi

PORT=8505

# If already running, just open the browser
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

echo "  Supports: HCP data, HCO data, or combined files"
echo "  (Keep this window open while using the app)"
echo "  Press Ctrl+C to stop the app"
echo ""

# Open browser after a short delay (gives Streamlit time to start)
(sleep 3 && open http://localhost:$PORT) &

# Launch Streamlit from this directory (1GB upload limit for large files)
streamlit run app.py \
    --server.port $PORT \
    --server.headless true \
    --server.maxUploadSize 1000 \
    --browser.gatherUsageStats false \
    --theme.base dark

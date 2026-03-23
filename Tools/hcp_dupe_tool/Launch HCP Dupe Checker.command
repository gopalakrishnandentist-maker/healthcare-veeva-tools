#!/bin/bash
# ─────────────────────────────────────────────────────
#  HCP Duplicate Identification Tool — Mac Launcher
#  Double-click this file to start the app
# ─────────────────────────────────────────────────────

# Get the directory where this script lives
DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "  ╔═══════════════════════════════════════════════╗"
echo "  ║   HCP Duplicate Identification Tool v2.0      ║"
echo "  ║   Starting...                                 ║"
echo "  ╚═══════════════════════════════════════════════╝"
echo ""

# Move to the parent folder (the one containing hcp_dupe_tool/)
cd "$DIR/.."

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

echo "  (Keep this window open while using the app)"
echo "  Press Ctrl+C to stop the app"
echo ""

PORT=8509

# If already running, just open the browser
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

# Open browser after a short delay (gives Streamlit time to start)
(sleep 3 && open http://localhost:$PORT) &

# Launch Streamlit (1GB upload limit for large files)
streamlit run hcp_dupe_tool/app.py \
    --server.port $PORT \
    --server.headless true \
    --server.maxUploadSize 1000 \
    --browser.gatherUsageStats false \
    --theme.base dark

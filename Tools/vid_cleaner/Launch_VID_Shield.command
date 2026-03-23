#!/bin/bash

# ══════════════════════════════════════════════════════════
#  VID Data Shield v2.1 — Double-Click Launcher for macOS
# ══════════════════════════════════════════════════════════

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

clear
echo ""
echo "  ╔══════════════════════════════════════════════╗"
echo "  ║         VID Data Shield  v2.1                ║"
echo "  ║   Protect 18-digit Veeva IDs  |  No Size Limit  ║"
echo "  ╚══════════════════════════════════════════════╝"
echo ""

# ── Check Python ──────────────────────────────────────────
PYTHON=""
if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
else
    echo "  ERROR: Python not found."
    echo "  Install Python 3 from https://python.org"
    echo ""
    read -p "  Press Enter to close..."
    exit 1
fi

echo "  Python: $($PYTHON --version 2>&1)"

# ── Check dependencies ────────────────────────────────────
if ! $PYTHON -c "import streamlit" 2>/dev/null; then
    echo ""
    echo "  Installing dependencies (one-time)..."
    $PYTHON -m pip install streamlit pandas openpyxl xlsxwriter --quiet
    echo "  Done."
fi

# ── Check app exists ──────────────────────────────────────
if [ ! -f "$DIR/vid_shield.py" ]; then
    echo ""
    echo "  ERROR: vid_shield.py not found next to this launcher."
    read -p "  Press Enter to close..."
    exit 1
fi

# ── Launch ────────────────────────────────────────────────
echo ""
echo "  Launching in your browser..."
echo "  No file size limit — supports 800 MB+ files"
echo "  To stop: close this window or press Ctrl+C"
echo ""

PORT=8502
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

$PYTHON -m streamlit run "$DIR/vid_shield.py" \
    --server.port=$PORT \
    --server.headless=true \
    --server.maxUploadSize=10000 \
    --server.maxMessageSize=10000 \
    --browser.gatherUsageStats=false

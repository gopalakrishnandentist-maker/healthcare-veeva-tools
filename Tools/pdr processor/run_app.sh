#!/bin/bash
echo "============================================"
echo "  PDR Processing Dashboard - Launcher"
echo "============================================"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed."
    echo "Install it via: brew install python3 (Mac) or sudo apt install python3 (Linux)"
    exit 1
fi

# Check/install dependencies
echo "Checking dependencies..."
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo "Installing required packages (first-time only)..."
    pip3 install streamlit pandas openpyxl plotly rapidfuzz
    echo ""
fi

PORT=8506

# If already running, just open the browser
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || xdg-open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

# Launch the app
echo "Starting PDR Processing Dashboard..."
echo ""
echo "The app will open in your browser at http://localhost:$PORT"
echo "Press Ctrl+C to stop the app."
echo ""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
streamlit run "$SCRIPT_DIR/pdr_app.py" --server.port $PORT --server.headless true

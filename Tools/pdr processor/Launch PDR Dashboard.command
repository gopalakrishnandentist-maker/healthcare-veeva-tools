#!/bin/bash
echo "============================================"
echo "  PDR Processing Dashboard"
echo "============================================"
echo ""

# Navigate to the folder where this script lives
cd "$(dirname "$0")"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed."
    echo ""
    echo "Install it by running this in Terminal:"
    echo "  brew install python3"
    echo ""
    echo "Or download from https://python.org"
    read -p "Press Enter to close..."
    exit 1
fi

# Check/install dependencies
python3 -c "import streamlit" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "First-time setup: installing required packages..."
    echo "(This only happens once)"
    echo ""
    pip3 install streamlit pandas openpyxl plotly rapidfuzz
    echo ""
    echo "Setup complete!"
    echo ""
fi

# Ensure .streamlit config exists (raises upload limit to 1GB)
if [ ! -d ".streamlit" ]; then
    mkdir -p .streamlit
    cat > .streamlit/config.toml << 'CONF'
[server]
maxUploadSize = 1000
maxMessageSize = 1000

[browser]
gatherUsageStats = false
CONF
    echo "Created .streamlit/config.toml (upload limit: 1 GB)"
fi

PORT=8506

# If already running, just open the browser
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "  App is already running — opening browser..."
    open "http://localhost:$PORT" 2>/dev/null || true
    exit 0
fi

# Launch
echo "Opening dashboard in your browser..."
echo "URL: http://localhost:$PORT"
echo ""
echo "Keep this window open while using the dashboard."
echo "Press Ctrl+C here to stop."
echo ""
echo "TIP: For NWK files > 200MB, use 'Local file path' mode in the sidebar"
echo "     instead of uploading — it loads directly from disk with no size limit."
echo ""
python3 -m streamlit run pdr_app.py --server.port $PORT --server.headless true

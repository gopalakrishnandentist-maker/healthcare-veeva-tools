# PDR Processing Tool — Setup & Operations Guide

## One-Time Setup (5 minutes)

### 1. Install Python
If not already installed, download Python 3.10+ from python.org

### 2. Install Dependencies
Open a terminal/command prompt and run:
```
pip install streamlit pandas openpyxl plotly rapidfuzz
```

### 3. Place Files
Save these two files in the same folder on your machine:
- `pdr_processor.py` — Core processing engine (also works standalone via CLI)
- `pdr_app.py` — Streamlit dashboard app

---

## Daily Operations — Step by Step

### Step 1: Export PDR Batch from Veeva
- Navigate to your PDR task queue in Veeva Network
- Export as Excel (.xlsx) with all columns
- Save to a known location (e.g., `C:\PDR_Processing\input\`)

### Step 2: Export NWK Reference (weekly)
- Export existing HCP records from Veeva Network
- Key columns needed: VID, First Name, Last Name, Specialty 1, City (CDA), Parent HCO Name
- Save as Excel (.xlsx)
- You only need to refresh this weekly (or at sprint start), not daily

### Step 3: Launch the Dashboard
Open a terminal and run:
```
streamlit run pdr_app.py
```
This opens the dashboard in your browser at http://localhost:8501

### Step 4: Upload & Process
1. Upload the PDR export file in the sidebar
2. Upload the NWK reference file in the sidebar
3. Adjust thresholds if needed (defaults are good to start)
4. Click **Process PDRs**
5. Wait for processing to complete (est. 5-15 min for 9K PDRs against 600K NWK)

### Step 5: Review Dashboard
- Check the **Dashboard** tab for tier distribution and projected time savings
- Spot-check a few Tier 1 auto-resolved items for accuracy

### Step 6: Export & Distribute
- Download the **Full Results** Excel from the Export tab
- Optionally download **Tier-specific** exports to assign to different steward teams
- Distribute to stewards via your normal channel (email, shared drive, etc.)

### Step 7: Steward Workflow
Stewards open the enriched Excel and:
1. **Sort by AI_Tier** — start with Tier 2 items (AI pre-processed)
2. **Tier 1 items**: Already resolved. Senior steward spot-checks 5-10%
3. **Tier 2 items**: Review the AI recommendation, confirm or override, then process in Veeva
4. **Tier 3 items**: Handle manually with AI-provided context (phone numbers, match details)

---

## Alternative: CLI Mode (no Streamlit)

If you prefer command-line:
```
python pdr_processor.py --pdr pdrs.xlsx --nwk nwk_export.xlsx --output processed.xlsx
```

Options:
```
--exact-threshold 92    # Score above this = auto-reject duplicate
--partial-threshold 70  # Score between this and exact = flag for review
```

---

## Threshold Tuning Guide

| Threshold | Default | Raise it if... | Lower it if... |
|-----------|---------|----------------|----------------|
| Exact Match | 92 | Too many false duplicate flags | Missing obvious duplicates |
| Partial Match | 70 | Too many records sent to review | Stewards want more cases flagged |

**Recommended approach**: Start with defaults, process a batch of ~500 PDRs, have a senior steward validate the Tier 1 auto-resolutions. If accuracy is >98%, keep defaults. If false positives appear, raise the exact threshold to 95.

---

## Performance Notes

| PDR Volume | NWK Size | Est. Processing Time | RAM Usage |
|-----------|----------|---------------------|-----------|
| 1,000 | 100K | ~1-2 min | ~300 MB |
| 5,000 | 300K | ~5-8 min | ~600 MB |
| 9,000 | 600K | ~10-15 min | ~1 GB |

- **rapidfuzz** (recommended): 10x faster fuzzy matching than difflib
- **Blocking strategy**: Only compares within name-prefix + city blocks, not all pairs
- NWK index is built once per run; subsequent PDR processing is fast

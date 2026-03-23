# HCP Data Validator Tool

**Version:** 2.0
**Author:** Built for OpenData India Operations at Veeva Systems
**Purpose:** Validate HCP License, Candidate, Affiliation, and Status data at scale

---

## Overview

The HCP Data Validator is a comprehensive GUI tool designed to validate Healthcare Professional (HCP) data for four critical quality checks:

1. **License Check** - Identifies VIDs without active licenses (status = "Active" AND license number is not blank/0)
2. **Candidate Check** - Identifies records marked as candidates requiring review (candidate_record = True)
3. **Active Affiliation Check** - Identifies VIDs without active HCO affiliations (non-empty parent HCO AND status != "Inactive")
4. **HCP Status Check** - Identifies VIDs where HCP status is not "Active"

This tool is optimized to handle large datasets (150K+ rows) efficiently.

---

## What's New in v2.0

- Modern sidebar-based UI with dashboard cards and donut charts
- New HCP Status validation check (Rule: all HCPs must have Active status)
- Fixed .xls file loading (legacy format now auto-detected)
- Fixed data consistency bugs in affiliation check
- Professional formatted Excel exports with styled headers
- Matplotlib-powered visual charts on the dashboard
- Alternating row colors and sortable data tables
- Empty file validation on load

---

## Validation Rules (All 4 Must Pass)

Every HCP in your dataset must satisfy:

1. At least 1 active License Number that is not just "0"
2. At least 1 Active Affiliation to a HCO
3. HCP Status must be "Active"
4. Candidate Record status must be "False"

---

## Features

- **Modern GUI** - Sidebar navigation with dashboard, detail pages, and visual charts
- **Multi-threaded Processing** - Handles large datasets without freezing
- **Flexible File Support** - Supports Excel (.xlsx, .xls) and CSV files
- **4 Validation Checks** - Run one or all checks at once
- **Visual Dashboard** - Metric cards, donut charts, and summary tables
- **Formatted Excel Export** - Styled headers, auto-width columns, frozen panes
- **Progress Tracking** - Real-time status updates and progress indicators

---

## Requirements

- Python 3.7 or higher
- pandas (for data processing)
- openpyxl (for Excel file handling)
- matplotlib (for dashboard charts)
- tkinter (usually comes with Python)

---

## Installation

### Step 1: Install Python
Download and install Python from [python.org](https://www.python.org/downloads/)

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

Or manually:
```bash
pip install pandas openpyxl matplotlib
```

### Step 3: Run the Tool
```bash
python hcp_data_validator.py
```

---

## How to Use

### 1. Load Your Data File
- Click **"Browse File"** in the sidebar
- Select your HCP data file (.xlsx, .xls, or .csv)
- Wait for the file to load (status shows row count)

### 2. Select Validation Checks
Navigate to the **Dashboard** page and choose which checks to run:

- License Check
- Candidate Check
- Affiliation Check
- HCP Status Check

### 3. Run Analysis
- Click **"Run Analysis"** button
- Wait for processing (progress bar shows activity)
- Review results on the Dashboard and individual detail pages

### 4. Export Results
- Click **"Export to Excel"** in the sidebar
- Choose save location
- Open the exported file for detailed analysis

---

## Expected File Columns

The tool expects these columns (case-sensitive):

**HCP Information:**
- `hcp.vid__v (VID)` - Required
- `hcp.first_name__v (FIRST NAME)` - Optional
- `hcp.last_name__v (LAST NAME)` - Optional
- `hcp.candidate_record__v (CANDIDATE RECORD)` - For Candidate Check
- `hcp.ap_candidate_rejection_reason__c (CANDIDATE REVIEW RESULT)` - Optional
- `hcp.hcp_status__v (HCP STATUS)` - For HCP Status Check

**License Information:**
- `license.license_number__v (LICENSE)` - Optional but recommended
- `license.license_status__v (LICENSE STATUS)` - Required for License Check

**Affiliation Information:**
- `hco.parent_hco_vid__v (PARENT_HCO_VID__V)` - For Affiliation Check
- `hco.parent_hco_status__v (PARENT_HCO_STATUS__V)` - For Affiliation Check

---

## Export File Structure

The exported Excel file contains multiple sheets:

1. **Summary** - Overview of all validation results
2. **License Issues** - VIDs without active licenses
3. **All VID License Counts** - Complete license count for all VIDs
4. **Candidate Records** - Records flagged as candidates
5. **Affiliation Issues** - VIDs without active affiliations
6. **All VID Affiliation Counts** - Complete affiliation count for all VIDs
7. **HCP Status Issues** - VIDs with non-Active status

---

## Performance

- **150K rows:** ~10-20 seconds (depending on system)
- **Memory efficient:** Uses pandas groupby operations
- **Non-blocking UI:** Multi-threaded processing keeps interface responsive

---

## Version History

**v2.0** (2026-02-07)
- Added HCP Status validation check
- Complete UI redesign with modern sidebar navigation
- Dashboard with metric cards and donut charts
- Fixed .xls file loading bug
- Fixed affiliation check data consistency
- Added matplotlib chart visualizations
- Professional Excel export formatting

**v1.0** (2026-01-30)
- Initial release
- License, Candidate, and Affiliation checks
- Basic tkinter GUI

---

**Built for Veeva Systems - OpenData India Operations**

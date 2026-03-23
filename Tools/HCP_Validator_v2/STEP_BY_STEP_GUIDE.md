# HCP Data Validator - Complete Step-by-Step Guide

## 📋 Table of Contents
1. [Before You Start](#before-you-start)
2. [Installation Steps](#installation-steps)
3. [Launching the Tool](#launching-the-tool)
4. [Using the Tool](#using-the-tool)
5. [Understanding Results](#understanding-results)
6. [Exporting Results](#exporting-results)
7. [Troubleshooting](#troubleshooting)

---

## Before You Start

### ✅ Checklist
- [ ] You have Python installed (version 3.7 or higher)
- [ ] You have your HCP data file ready (Excel or CSV)
- [ ] You have downloaded all 4 files:
  - `hcp_data_validator.py`
  - `requirements.txt`
  - `README.md`
  - `QUICKSTART.md`

### 📁 File Organization
Create a folder and put all downloaded files there:
```
HCP_Validator/
├── hcp_data_validator.py
├── requirements.txt
├── README.md
└── QUICKSTART.md
```

---

## Installation Steps

### Step 1: Check Python Installation

**Windows:**
1. Press `Windows Key + R`
2. Type `cmd` and press Enter
3. In the black window (Command Prompt), type:
   ```
   python --version
   ```
4. You should see something like: `Python 3.10.5`

**Mac:**
1. Press `Command + Space`
2. Type `Terminal` and press Enter
3. In the Terminal, type:
   ```
   python3 --version
   ```
4. You should see something like: `Python 3.10.5`

**If you DON'T have Python:**
- Go to https://www.python.org/downloads/
- Download and install the latest version
- During installation, **CHECK** "Add Python to PATH"

---

### Step 2: Install Required Packages

**Windows:**
1. Open Command Prompt (Windows Key + R, type `cmd`, press Enter)
2. Navigate to your tool folder:
   ```
   cd C:\Users\YourName\Desktop\HCP_Validator
   ```
   (Replace with your actual folder path)
   
3. Install packages:
   ```
   pip install pandas openpyxl
   ```

4. Wait for installation (you'll see download progress)
5. When done, you'll see "Successfully installed pandas-X.X.X openpyxl-X.X.X"

**Mac:**
1. Open Terminal (Command + Space, type "Terminal")
2. Navigate to your tool folder:
   ```
   cd ~/Desktop/HCP_Validator
   ```
   (Replace with your actual folder path)
   
3. Install packages:
   ```
   pip3 install pandas openpyxl
   ```

4. Wait for installation
5. When done, you'll see "Successfully installed pandas-X.X.X openpyxl-X.X.X"

**Alternative Method (Using requirements.txt):**
```
pip install -r requirements.txt
```
or on Mac:
```
pip3 install -r requirements.txt
```

---

## Launching the Tool

### Method 1: From Command Line (Recommended)

**Windows:**
1. Open Command Prompt
2. Navigate to tool folder:
   ```
   cd C:\Users\YourName\Desktop\HCP_Validator
   ```
3. Run the tool:
   ```
   python hcp_data_validator.py
   ```

**Mac:**
1. Open Terminal
2. Navigate to tool folder:
   ```
   cd ~/Desktop/HCP_Validator
   ```
3. Run the tool:
   ```
   python3 hcp_data_validator.py
   ```

### Method 2: Double-Click (Windows Only)

**Option A: If .py files are associated with Python:**
- Simply double-click `hcp_data_validator.py`
- The GUI will open

**Option B: Create a shortcut:**
1. Right-click `hcp_data_validator.py`
2. Select "Open with" → "Choose another app"
3. Select Python
4. Check "Always use this app"
5. Click OK

### Method 3: Using Python IDLE

1. Right-click `hcp_data_validator.py`
2. Select "Edit with IDLE"
3. Press `F5` or go to Run → Run Module

---

## Using the Tool

### 🎯 Main Interface Overview

When the tool launches, you'll see:

```
┌──────────────────────────────────────────────────────────┐
│         HCP Data Validator                               │
│    OpenData India Operations | Veeva Systems             │
└──────────────────────────────────────────────────────────┘

┌─ 1. Load Data File ────────────────────────────────────┐
│  ○ No file selected                [Browse Excel File] │
└────────────────────────────────────────────────────────┘

┌─ 2. Select Validation Checks ──────────────────────────┐
│  ☑ License Check (VIDs without active licenses)        │
│  ☑ Candidate Check (Records marked as Candidate)       │
│  ☑ Active Affiliation Check (VIDs without active...)   │
└────────────────────────────────────────────────────────┘

              [▶ Run Analysis]  (disabled until file loaded)

┌─ 3. Validation Results ────────────────────────────────┐
│  [📊 Summary] [📋 License] [👤 Candidate] [🏥 Affil.]  │
│  (Empty until analysis is run)                          │
└────────────────────────────────────────────────────────┘

              [📊 Export Results to Excel]  (disabled)
```

---

### Step 1: Load Your Data File

**1.1 Click "Browse Excel File" button**
   - Location: Top section, right side

**1.2 Navigate to your HCP data file**
   - Browse to where your file is saved
   - Supported formats: .xlsx, .xls, .csv

**1.3 Select your file and click "Open"**

**1.4 Wait for file to load**
   - You'll see: "Loading file: yourfile.xlsx..."
   - A progress message appears at the bottom

**1.5 Confirm successful load**
   - File name turns GREEN with checkmark: "✓ yourfile.xlsx"
   - Row/column count appears: "150,000 rows | 89 columns"
   - A popup confirms: "Successfully loaded file..."
   - "Run Analysis" button becomes ACTIVE (green)

**What if loading fails?**
- Close your Excel file if it's open
- Check file isn't corrupted
- Try saving as a new file

---

### Step 2: Select Validation Checks

**2.1 Review the three available checks:**

☑ **License Check**
   - Purpose: Finds VIDs without any active licenses
   - Rule: Checks if license status = "Active" AND license number not blank/0
   - Checked by default

☑ **Candidate Check**
   - Purpose: Finds records marked as candidates
   - Rule: Checks if candidate_record = True
   - Checked by default

☑ **Active Affiliation Check**
   - Purpose: Finds VIDs without active HCO affiliations
   - Rule: Checks if parent HCO exists AND status ≠ Inactive
   - Checked by default

**2.2 Choose which checks to run:**
   - Click checkbox to UNCHECK if you don't want a particular check
   - Click checkbox to CHECK if you want to enable it
   - You can run 1, 2, or all 3 checks at once

**2.3 Recommended approach:**
   - **For initial QC:** Run all 3 checks together
   - **For specific issues:** Run individual checks as needed
   - **For large files (500K+ rows):** Consider running separately

---

### Step 3: Run the Analysis

**3.1 Click "▶ Run Analysis" button**
   - Big green button in the center

**3.2 Watch the progress indicators:**
   - Progress bar animates (shows activity)
   - Status message updates:
     * "Running validation checks..."
     * "Checking licenses..."
     * "Checking candidate records..."
     * "Checking affiliations..."

**3.3 Processing time:**
   - 50K rows: ~5 seconds
   - 150K rows: ~10-20 seconds
   - 500K rows: ~45-60 seconds
   - UI remains responsive (you can see updates)

**3.4 Analysis complete:**
   - Progress bar stops
   - Status shows: "✓ Analysis complete!"
   - Popup message: "Validation checks completed successfully!"
   - Click "OK" on the popup

**3.5 Results appear automatically:**
   - Tabs are now populated with data
   - "Export Results to Excel" button becomes ACTIVE (red)

---

## Understanding Results

### Tab 1: 📊 Summary

**What you see:**
```
================================================================================
                      HCP DATA VALIDATION REPORT
                OpenData India Operations | Veeva Systems
================================================================================

Generated: 2026-01-30 15:45:30
Total Records: 150,000
File: HCP_Data_Jan2026.xlsx

================================================================================

📋 LICENSE CHECK RESULTS:
--------------------------------------------------------------------------------
   Total Unique VIDs: 45,230
   VIDs with Active License(s): 42,150
   ⚠️  VIDs WITHOUT Active License: 3,080
   Issue Rate: 6.81%

👤 CANDIDATE RECORD CHECK RESULTS:
--------------------------------------------------------------------------------
   ⚠️  Records Marked as Candidate: 1,245
   ℹ️  These records require review and validation

🏥 AFFILIATION CHECK RESULTS:
--------------------------------------------------------------------------------
   Total Unique VIDs: 45,230
   VIDs with Active Affiliation(s): 43,890
   ⚠️  VIDs WITHOUT Active Affiliation: 1,340
   Issue Rate: 2.96%

================================================================================
Next Steps:
  1. Review detailed results in individual tabs
  2. Export results to Excel for further analysis
  3. Share findings with data stewards and stakeholders
================================================================================
```

**How to interpret:**
- **Total Unique VIDs**: Number of distinct healthcare professionals in your file
- **Issue Rate**: Percentage of VIDs with problems (higher = more issues)
- **Warnings**: If issue rate > 10% for licenses or > 15% for affiliations

---

### Tab 2: 📋 License Issues

**What you see:**
```
VIDs WITHOUT Active Licenses
================================================================================
Total: 3,080 VIDs

                 VID  Count_of_Active_Licenses First_Name Last_Name
942095310354909937                         0      Lalit      Mani
942095310355172069                         0     Rakesh      Garg
942095310355565301                         0      Manoj Anand Gupta
...
```

**Columns explained:**
- **VID**: The healthcare professional's unique identifier
- **Count_of_Active_Licenses**: Should be 0 (that's why they're in this list)
- **First_Name / Last_Name**: Name of the HCP (for easy identification)

**What to do:**
1. Review each VID
2. Verify if they should have a license
3. Check source data for missing/incorrect license information
4. Flag for manual review or DCR submission

---

### Tab 3: 👤 Candidate Records

**What you see:**
```
CANDIDATE RECORDS
================================================================================
Total: 1,245 Candidate Records

These records need review and validation:

                 VID First_Name Last_Name Candidate_Record Review_Result
942095310354909937      Lalit      Mani             True          
942095310355172070      Mukta    Pujani             True   Approved
...
```

**Columns explained:**
- **VID**: Healthcare professional's ID
- **First_Name / Last_Name**: HCP name
- **Candidate_Record**: Always "True" (that's why they're here)
- **Review_Result**: May show approval status if available

**What to do:**
1. These are flagged records requiring manual validation
2. Review each candidate against source data
3. Approve or reject based on validation criteria
4. Update candidate status in system

---

### Tab 4: 🏥 Affiliation Issues

**What you see:**
```
VIDs WITHOUT Active HCO Affiliations
================================================================================
Total: 1,340 VIDs

                 VID  Count_of_Active_Affiliations First_Name   Last_Name
942095310354909937                             0      Lalit        Mani
942095310355172069                             0     Rakesh        Garg
...
```

**Columns explained:**
- **VID**: Healthcare professional's unique identifier
- **Count_of_Active_Affiliations**: Should be 0 (no active affiliations found)
- **First_Name / Last_Name**: Name of the HCP

**What to do:**
1. Check if HCP should have an affiliation
2. Verify parent HCO data exists and is correct
3. Ensure HCO status is "Active" not "Inactive"
4. Flag for affiliation data enrichment

---

## Exporting Results

### Step 1: Click "📊 Export Results to Excel"
   - Red button at the bottom
   - Only active after running analysis

### Step 2: Choose save location
   - A "Save As" dialog appears
   - Default name: `HCP_Validation_Results_20260130_154530.xlsx`
   - Navigate to where you want to save
   - You can change the filename if desired

### Step 3: Click "Save"
   - Tool creates the Excel file
   - Progress indicator may show briefly

### Step 4: Confirm export
   - Popup: "Results exported successfully to: [filepath]"
   - Click "OK"
   - Another popup: "Would you like to open the exported file?"
   - Click "Yes" to open immediately, "No" to open later

---

### What's in the Exported Excel File?

**Sheet 1: Summary**
- Overview metrics for all checks
- Issue rates and counts
- Perfect for management reports

**Sheet 2: License Issues**
- Only VIDs with 0 active licenses
- Includes names if available
- Ready for action/follow-up

**Sheet 3: All VID License Counts**
- COMPLETE list of all VIDs
- Shows license count for everyone (0, 1, 2, 3+)
- Good for comprehensive analysis

**Sheet 4: Candidate Records**
- All candidate records found
- Includes review status if available

**Sheet 5: Affiliation Issues**
- Only VIDs with 0 active affiliations
- Includes names if available

**Sheet 6: All VID Affiliation Counts**
- COMPLETE list of all VIDs
- Shows affiliation count for everyone
- Good for statistical analysis

**Use cases for exported file:**
- Share with team via email
- Upload to SharePoint/Drive
- Create pivot tables for further analysis
- Generate management reports
- Track issues in Excel
- Create action items for stewards

---

## Troubleshooting

### Problem: Tool won't launch

**Symptoms:** Double-clicking does nothing, or error appears

**Solutions:**
1. **Check Python installation:**
   ```
   python --version
   ```
   Should show Python 3.7+

2. **Check if packages installed:**
   ```
   pip list | findstr pandas
   pip list | findstr openpyxl
   ```
   Both should appear

3. **Launch from command line:**
   ```
   python hcp_data_validator.py
   ```
   Look for error messages

4. **Reinstall packages:**
   ```
   pip uninstall pandas openpyxl
   pip install pandas openpyxl
   ```

---

### Problem: "Required columns not found"

**Symptoms:** Error message when running analysis

**Solutions:**
1. **Check column names in your Excel:**
   - Open your data file
   - Look at the header row
   - Compare with required columns (see section below)

2. **Verify exact spelling:**
   - Column names are CASE-SENSITIVE
   - Must match exactly: `hcp.vid__v (VID)` not `hcp.vid (VID)`

3. **Check for extra spaces:**
   - No leading/trailing spaces in column names
   - No double spaces

4. **Required columns by check:**
   - **License Check:** `hcp.vid__v (VID)`, `license.license_status__v (LICENSE STATUS)`
   - **Candidate Check:** `hcp.vid__v (VID)`, `hcp.candidate_record__v (CANDIDATE RECORD)`
   - **Affiliation Check:** `hcp.vid__v (VID)`, `hco.parent_hco_vid__v (PARENT_HCO_VID__V)`, `hco.parent_hco_status__v (PARENT_HCO_STATUS__V)`

---

### Problem: File won't load

**Symptoms:** Error when browsing/loading file

**Solutions:**
1. **Close file in Excel:**
   - File must not be open elsewhere
   - Check if anyone else has it open on network

2. **Check file format:**
   - Must be .xlsx, .xls, or .csv
   - Try saving as .xlsx

3. **Check file size:**
   - Very large files (>500MB) may be slow
   - Consider splitting if > 1 million rows

4. **Check file integrity:**
   - Try opening in Excel first
   - If corrupted, restore from backup

5. **Try different file:**
   - Test with a small sample file first
   - If sample works, original file may be corrupted

---

### Problem: Analysis is very slow

**Symptoms:** Takes more than 2 minutes for 150K rows

**Solutions:**
1. **Close other programs:**
   - Free up RAM
   - Close Excel, Chrome, etc.

2. **Check file size:**
   - How many rows? (Show count after loading)
   - 500K+ rows will take longer

3. **Run checks separately:**
   - Uncheck 2 checks
   - Run one at a time

4. **Check computer specs:**
   - Tool needs at least 4GB RAM
   - 8GB+ recommended for large files

---

### Problem: Export fails

**Symptoms:** Error when exporting to Excel

**Solutions:**
1. **Check save location permissions:**
   - Can you save other files there?
   - Try saving to Desktop instead

2. **Check filename:**
   - No special characters: \ / : * ? " < > |
   - Use letters, numbers, underscores only

3. **Check disk space:**
   - Need at least 100MB free
   - Export file can be 10-50MB

4. **Close Excel:**
   - If you're trying to overwrite an open file
   - Close Excel completely

---

### Problem: Results look wrong

**Symptoms:** Numbers don't match expectations

**Solutions:**
1. **Understand the logic:**
   - **License Check:** VID needs at least ONE "Active" license with number
   - **Affiliation Check:** VID needs at least ONE affiliation with non-empty parent HCO AND non-"Inactive" status
   - **Candidate Check:** Shows ALL records where candidate = True

2. **Check for duplicates:**
   - Same VID can appear multiple times (multiple licenses/affiliations)
   - Tool counts unique VIDs correctly

3. **Verify source data:**
   - Open original Excel
   - Manually check a few VIDs
   - Compare with tool results

4. **Export and review:**
   - Export to Excel
   - Use Excel filters/pivot tables
   - Verify counts manually

---

### Problem: GUI looks weird

**Symptoms:** Buttons cut off, text overlapping

**Solutions:**
1. **Resize window:**
   - Drag corners to make larger
   - Minimum recommended: 1400x900 pixels

2. **Check screen resolution:**
   - Must be at least 1366x768
   - Higher is better

3. **Check display scaling:**
   - Windows: Right-click Desktop → Display Settings
   - Look for "Scale and layout"
   - Try 100% scaling

4. **Update Python:**
   - Old versions may have tkinter issues
   - Update to latest Python

---

## Tips & Best Practices

### ✅ Before Running Analysis

1. **Verify your file:**
   - Open in Excel first
   - Check column headers
   - Verify data looks correct

2. **Know what you're looking for:**
   - Decide which checks you need
   - Understand what "issues" mean for your use case

3. **Have clean data:**
   - No extra header rows
   - Column names in Row 1
   - Data starts in Row 2

### ✅ While Running Analysis

1. **Don't close the tool:**
   - Let it finish completely
   - Progress bar will stop when done

2. **Don't open the file:**
   - Keep Excel closed while analyzing
   - File is being read by the tool

3. **Note any errors:**
   - Take screenshot if error appears
   - Note what you were doing

### ✅ After Getting Results

1. **Review summary first:**
   - Get the big picture
   - Note issue rates

2. **Drill into details:**
   - Use individual tabs
   - Focus on specific VIDs

3. **Export immediately:**
   - Don't lose your results
   - Save with descriptive filename
   - Include date in filename

4. **Document findings:**
   - Create action items
   - Assign to stewards
   - Track resolution

### ✅ For Large Files (500K+ rows)

1. **Run checks separately:**
   - One check at a time
   - Reduces memory usage

2. **Consider splitting:**
   - Split into 2-3 smaller files
   - Run separately, combine results

3. **Allow more time:**
   - 500K rows: 1-2 minutes
   - 1M rows: 3-5 minutes

### ✅ Regular QC Workflow

**Weekly/Monthly routine:**
1. Load latest data extract
2. Run all 3 checks
3. Export results
4. Compare with previous week/month
5. Track improvements over time
6. Share summary with stakeholders

---

## Quick Reference Card

### Keyboard Shortcuts
- `Alt + F4` - Close tool (Windows)
- `Cmd + Q` - Close tool (Mac)

### File Locations
- Tool files: Where you saved them
- Export files: Where you chose to save
- Default export name: `HCP_Validation_Results_[datetime].xlsx`

### Common Commands

**Launch tool:**
```
Windows: python hcp_data_validator.py
Mac: python3 hcp_data_validator.py
```

**Install packages:**
```
Windows: pip install pandas openpyxl
Mac: pip3 install pandas openpyxl
```

**Check Python:**
```
Windows: python --version
Mac: python3 --version
```

### Expected Processing Times

| Rows    | Time       |
|---------|------------|
| 10K     | ~2 sec     |
| 50K     | ~5 sec     |
| 150K    | ~15 sec    |
| 500K    | ~60 sec    |
| 1M      | ~3 min     |

### When to Contact Support
- Tool crashes repeatedly
- Results are clearly incorrect
- Cannot install packages
- File won't load (after trying all solutions)
- Export fails (after trying all solutions)

---

## Need More Help?

**Documentation:**
- `README.md` - Comprehensive documentation
- `QUICKSTART.md` - Quick reference guide
- This guide - Step-by-step instructions

**Contact:**
- OpenData India Operations Team
- Your project manager
- IT Support for installation issues

---

**Last Updated:** January 30, 2026  
**Tool Version:** 1.0  
**Author:** Built for Veeva Systems - OpenData India Operations

---

🎉 **You're ready to use the HCP Data Validator!** 🎉

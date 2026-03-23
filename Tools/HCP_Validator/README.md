# HCP Data Validator Tool

**Version:** 1.0  
**Author:** Built for OpenData India Operations at Veeva Systems  
**Purpose:** Validate HCP License, Candidate, and Affiliation data at scale

---

## Overview

The HCP Data Validator is a comprehensive GUI tool designed to validate Healthcare Professional (HCP) data for three critical quality checks:

1. **License Check** - Identifies VIDs without active licenses
2. **Candidate Check** - Identifies records marked as candidates requiring review
3. **Active Affiliation Check** - Identifies VIDs without active HCO affiliations

This tool is optimized to handle large datasets (150K+ rows) efficiently.

---

## Features

✅ **User-Friendly GUI** - Intuitive interface with no coding required  
✅ **Multi-threaded Processing** - Handles large datasets without freezing  
✅ **Flexible File Support** - Supports Excel (.xlsx, .xls) and CSV files  
✅ **Multiple Validation Checks** - Run one or all checks at once  
✅ **Detailed Results** - View results in organized tabs  
✅ **Excel Export** - Export all results to a formatted Excel workbook  
✅ **Progress Tracking** - Real-time status updates and progress indicators  

---

## Requirements

- Python 3.7 or higher
- pandas (for data processing)
- openpyxl (for Excel file handling)
- tkinter (usually comes with Python)

---

## Installation

### Step 1: Install Python
Download and install Python from [python.org](https://www.python.org/downloads/)

### Step 2: Install Dependencies
Open your terminal/command prompt and run:

```bash
pip install pandas openpyxl
```

Or use the requirements file:

```bash
pip install -r requirements.txt
```

### Step 3: Run the Tool
```bash
python hcp_data_validator.py
```

---

## How to Use

### 1. Load Your Data File
- Click **"Browse Excel File"** button
- Select your HCP data file (.xlsx, .xls, or .csv)
- Wait for the file to load (status shows row count)

### 2. Select Validation Checks
Choose which checks you want to run:

- ✓ **License Check** - Finds VIDs with no active licenses
- ✓ **Candidate Check** - Finds records marked as candidates
- ✓ **Affiliation Check** - Finds VIDs with no active HCO affiliations

### 3. Run Analysis
- Click **"▶ Run Analysis"** button
- Wait for processing (progress bar shows activity)
- Review results in the tabs

### 4. Export Results
- Click **"📊 Export Results to Excel"** button
- Choose save location
- Open the exported file for detailed analysis

---

## Validation Rules

### License Check
**Goal:** Ensure all VIDs have at least one active license

**Conditions for Active License:**
- `license.license_status__v (LICENSE STATUS)` = "Active"
- AND `license.license_number__v (LICENSE)` is NOT blank/0

**Output:**
- VID
- Count_of_Active_Licenses (0 means no active license)
- First_Name (if available)
- Last_Name (if available)

---

### Candidate Check
**Goal:** Identify records flagged as candidates for review

**Condition:**
- `hcp.candidate_record__v (CANDIDATE RECORD)` = True

**Output:**
- VID
- First_Name
- Last_Name
- Candidate_Record status
- Candidate_Review_Result (if available)

---

### Active Affiliation Check
**Goal:** Ensure all VIDs have at least one active HCO affiliation

**Conditions for Active Affiliation:**
- `hco.parent_hco_vid__v (PARENT_HCO_VID__V)` is NOT empty
- AND `hco.parent_hco_status__v (PARENT_HCO_STATUS__V)` is NOT "Inactive"

**Output:**
- VID
- Count_of_Active_Affiliations (0 means no active affiliation)
- First_Name (if available)
- Last_Name (if available)

---

## Expected File Columns

The tool expects these columns (case-sensitive):

**HCP Information:**
- `hcp.vid__v (VID)` - Required
- `hcp.first_name__v (FIRST NAME)` - Optional
- `hcp.last_name__v (LAST NAME)` - Optional
- `hcp.candidate_record__v (CANDIDATE RECORD)` - For Candidate Check
- `hcp.ap_candidate_rejection_reason__c (CANDIDATE REVIEW RESULT)` - Optional

**License Information:**
- `license.license_number__v (LICENSE)` - Optional but recommended
- `license.license_status__v (LICENSE STATUS)` - Required for License Check

**Affiliation Information:**
- `hco.parent_hco_vid__v (PARENT_HCO_VID__V)` - For Affiliation Check
- `hco.parent_hco_status__v (PARENT_HCO_STATUS__V)` - For Affiliation Check
- `hco.parent_hco_name__v (PARENT_HCO_NAME__V)` - Optional

---

## Export File Structure

The exported Excel file contains multiple sheets:

1. **Summary** - Overview of all validation results
2. **License Issues** - VIDs without active licenses
3. **All VID License Counts** - Complete license count for all VIDs
4. **Candidate Records** - Records flagged as candidates
5. **Affiliation Issues** - VIDs without active affiliations
6. **All VID Affiliation Counts** - Complete affiliation count for all VIDs

---

## Performance

- **150K rows:** ~10-20 seconds (depending on system)
- **Memory efficient:** Uses pandas groupby operations
- **Non-blocking UI:** Multi-threaded processing keeps interface responsive

---

## Troubleshooting

### "Required columns not found"
- Verify your file has the exact column names (case-sensitive)
- Check the column headers match the expected format
- Ensure you're using the correct file

### "File failed to load"
- Ensure the file is not open in Excel
- Check file is not corrupted
- Try saving as a new file and loading again

### "No results to export"
- Run analysis first before exporting
- Ensure at least one validation check is selected

---

## Use Cases

### Quality Control
Run regular checks on new HCP data batches to identify data quality issues before processing.

### Data Stewardship
Identify records requiring manual review and validation.

### Compliance Verification
Ensure all HCPs have proper licensing and affiliations as per regulatory requirements.

### DCR Performance Monitoring
Track data quality metrics over time by running regular validation checks.

---

## Technical Details

**Architecture:**
- GUI: tkinter (Python standard library)
- Data Processing: pandas with optimized groupby operations
- Multi-threading: Background processing for large datasets
- File I/O: openpyxl for Excel, pandas for CSV

**Key Design Principles:**
- Handle multiple licenses per VID (count active ones)
- Handle multiple affiliations per VID (count active ones)
- Support flexible column naming
- Maintain data integrity throughout processing
- Provide clear, actionable results

---

## Future Enhancements

Potential additions for future versions:
- Support for additional data validations
- Automated scheduling for regular checks
- Integration with Veeva data pipelines
- Custom validation rule configuration
- Historical trend analysis

---

## Support

For questions or issues:
- Contact: OpenData India Operations Team
- Email: [Your Team Email]

---

## Version History

**v1.0** (2026-01-30)
- Initial release
- License validation check
- Candidate record check
- Active affiliation check
- Excel export functionality
- Multi-threaded processing for large datasets

---

## License

Internal tool for Veeva Systems - OpenData India Operations  
© 2026 Veeva Systems

---

**Built with ❤️ for Data Quality Excellence**

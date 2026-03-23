# HCP Data Validator - Quick Start Guide

## 🚀 Quick Start (3 Steps)

### 1. Install Dependencies
```bash
pip install pandas openpyxl
```

### 2. Run the Tool
```bash
python hcp_data_validator.py
```

### 3. Use the Tool
1. Click "Browse Excel File" → Select your HCP data file
2. Select validation checks (License, Candidate, Affiliation)
3. Click "▶ Run Analysis"
4. Review results in tabs
5. Click "📊 Export Results to Excel"

---

## 📋 What Each Check Does

### License Check
✓ Finds VIDs without any active licenses  
✓ Checks: Status = "Active" AND License Number is not blank/0  
✓ Output: VID + Count of Active Licenses

### Candidate Check
✓ Finds records marked as candidates  
✓ Checks: Candidate Record = True  
✓ Output: VID + Candidate status

### Affiliation Check
✓ Finds VIDs without active HCO affiliations  
✓ Checks: Parent HCO VID exists AND Status ≠ "Inactive"  
✓ Output: VID + Count of Active Affiliations

---

## 📊 Required Excel Columns

**Minimum Required:**
- `hcp.vid__v (VID)`
- `license.license_status__v (LICENSE STATUS)` - for License Check
- `hcp.candidate_record__v (CANDIDATE RECORD)` - for Candidate Check
- `hco.parent_hco_vid__v (PARENT_HCO_VID__V)` - for Affiliation Check
- `hco.parent_hco_status__v (PARENT_HCO_STATUS__V)` - for Affiliation Check

**Optional but Recommended:**
- `hcp.first_name__v (FIRST NAME)`
- `hcp.last_name__v (LAST NAME)`
- `license.license_number__v (LICENSE)`

---

## ⚡ Performance
- **150K rows**: ~10-20 seconds
- **Multi-threaded**: UI stays responsive
- **Memory efficient**: Optimized pandas operations

---

## 🔧 Troubleshooting

**Problem:** "Required columns not found"  
**Solution:** Check column names match exactly (case-sensitive)

**Problem:** File won't load  
**Solution:** Close file in Excel, try again

**Problem:** Analysis takes too long  
**Solution:** For 500K+ rows, consider splitting file

---

## 📁 Export File Contents

1. **Summary** - Overall metrics
2. **License Issues** - VIDs needing attention
3. **Candidate Records** - Records to review
4. **Affiliation Issues** - VIDs without affiliations
5. **All Counts** - Complete data for all VIDs

---

## 💡 Pro Tips

✅ Run all three checks at once for complete validation  
✅ Export results immediately for documentation  
✅ Use exported Excel file for detailed analysis  
✅ Keep original files - tool reads only, doesn't modify  
✅ For large files (500K+), run checks separately if needed

---

## 🎯 Common Use Cases

1. **New Data Batch QC**: Validate before processing
2. **Monthly Audits**: Regular data quality checks
3. **Client Deliverables**: Pre-delivery validation
4. **DCR Performance**: Track data quality metrics
5. **Compliance**: Verify licensing requirements

---

Need more details? See **README.md** for comprehensive documentation!

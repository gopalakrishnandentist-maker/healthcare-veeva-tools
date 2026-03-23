# Quick Command Reference - HCP Data Validator

## 🚀 Installation & Launch (Copy-Paste Ready)

### Windows Users

**1. Install Packages:**
```batch
pip install pandas openpyxl
```

**2. Launch Tool:**
```batch
cd C:\Users\YourName\Desktop\HCP_Validator
python hcp_data_validator.py
```

### Mac Users

**1. Install Packages:**
```bash
pip3 install pandas openpyxl
```

**2. Launch Tool:**
```bash
cd ~/Desktop/HCP_Validator
python3 hcp_data_validator.py
```

---

## 📂 Required Column Names (Exact Match)

### For License Check:
```
hcp.vid__v (VID)
license.license_status__v (LICENSE STATUS)
license.license_number__v (LICENSE)
```

### For Candidate Check:
```
hcp.vid__v (VID)
hcp.candidate_record__v (CANDIDATE RECORD)
```

### For Affiliation Check:
```
hcp.vid__v (VID)
hco.parent_hco_vid__v (PARENT_HCO_VID__V)
hco.parent_hco_status__v (PARENT_HCO_STATUS__V)
```

### Optional (but recommended):
```
hcp.first_name__v (FIRST NAME)
hcp.last_name__v (LAST NAME)
```

---

## 🔧 Troubleshooting Commands

### Check if Python is installed:
```
python --version
```
or
```
python3 --version
```

### Check if packages are installed:
```
pip list | findstr pandas
pip list | findstr openpyxl
```
or on Mac/Linux:
```
pip3 list | grep pandas
pip3 list | grep openpyxl
```

### Reinstall packages if needed:
```
pip uninstall pandas openpyxl
pip install pandas openpyxl
```

---

## 📊 Processing Speed Reference

| Rows     | Expected Time |
|----------|---------------|
| 50K      | ~5 seconds    |
| 150K     | ~15 seconds   |
| 500K     | ~60 seconds   |
| 1M       | ~3 minutes    |

---

## 💡 Common Issues & Quick Fixes

**Issue:** "python" is not recognized
**Fix:** Use `python3` instead, or reinstall Python with "Add to PATH" checked

**Issue:** "Required columns not found"
**Fix:** Check exact column names (case-sensitive) - see above

**Issue:** File won't load
**Fix:** Close file in Excel, try again

**Issue:** Tool is slow
**Fix:** Close other programs, check file size

**Issue:** Export fails
**Fix:** Check save location permissions, close Excel

---

## 📞 Support Contacts

- **Installation issues:** IT Support
- **Data questions:** OpenData India Operations Team
- **Tool questions:** Project Manager

---

**Keep this handy!** Pin to your desktop or print for quick reference.

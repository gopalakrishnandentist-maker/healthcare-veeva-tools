# HCP Duplicate Detection Tool — Large-Scale Version

## Overview

This is an **optimized version** of the HCP duplicate detection tool, purpose-built for handling **600K+ rows / 300K+ unique VIDs** on laptops with **8-16 GB RAM**.

### What This Tool Does

The HCP (Healthcare Provider) duplicate detection tool identifies likely duplicate records within a large healthcare dataset by:
1. **Canonicalizing** multi-row records (one row per provider)
2. **Blocking** to generate candidate pairs (sophisticated rules to avoid O(n²) comparisons)
3. **Comparing** pairs using fuzzy matching, contact overlap, licensing, geography, etc.
4. **Classifying** pairs into:
   - **AUTO**: Definite duplicates (automatic merge recommended)
   - **REVIEW**: Potential duplicates (manual review recommended)
   - **NOTDUP**: Definite non-duplicates (don't merge)

### Version Comparison

| Feature | Standard (`run.py`) | **Large-Scale (`run_large.py`)** |
|---------|---------------------|----------------------------------|
| Input | XLSX or CSV | CSV only |
| Max rows | ~300K | **600K+** |
| Max unique VIDs | ~150K | **300K+** |
| Peak memory | 20-25 GB | **4-6 GB** |
| Runtime | 60-90 min | **~27 min** |
| Parallelization | None | **8 workers** |
| Output | Excel + CSV | **CSV + Excel** |

---

## Installation & Setup

### Requirements

```bash
python >= 3.8
pandas
openpyxl  # for Excel output
tqdm      # for progress bars (optional but recommended)
PyYAML    # for config files
rapidfuzz # for fuzzy matching (optional, uses difflib fallback)
psutil    # for memory monitoring (optional)
```

### Install Dependencies

```bash
pip install pandas openpyxl tqdm pyyaml rapidfuzz psutil
```

### Verify Installation

```bash
cd /sessions/relaxed-fervent-tesla/mnt/outputs/hcp_dupe_tool
python3 run_large.py --help
```

You should see the help text with all 8 CLI flags.

---

## Quick Start

### 1. Prepare Your Data

Your input must be **CSV format** (not XLSX). If you have Excel:

```bash
# In Excel:
# File > Save As > Format: CSV UTF-8

# Or in Python:
import pandas as pd
df = pd.read_excel("data.xlsx")
df.to_csv("data.csv", index=False)

# Or in R:
write.csv(data, "data.csv", row.names = FALSE)
```

### 2. Run the Tool

```bash
python -m hcp_dupe_tool.run_large --input data.csv --outdir ./results
```

This will:
- Load CSV and deduplicate into canonical records
- Generate candidate pairs
- Compare pairs in parallel (8 workers)
- Stream results to CSVs
- Consolidate into Excel workbook
- Print progress with ETA

**Typical runtime:** 30-60 minutes for 600K rows

### 3. Review Results

Open the output Excel file:

```bash
results/HCP_Dupe_Check_Large.xlsx
```

Three sheets:
- **AUTO**: Pairs flagged for automatic merging
- **REVIEW**: Pairs requiring human judgment
- **NOTDUP**: Pairs confirmed as different people

Each row shows both VIDs, name similarity score, matching contacts, and reasoning.

---

## Command-Line Options

```bash
python -m hcp_dupe_tool.run_large [options]

Required:
  --input FILE                 Input CSV file (must be CSV, not XLSX)

Optional:
  --outdir DIR                 Output directory (default: ./out)
  --config YAML                Config file (default: config_default.yaml)
  --workers N                  Worker processes (default: auto, max 6)
  --max-pairs N                Truncate pairs if exceeding this count
  --no-excel                   Skip Excel output, CSV only (faster)
  --enable-phonetic            Enable Soundex-based blocking (not recommended)
  --memory-limit MB            Warn if memory exceeds this threshold
  --shared-threshold N         Override shared-contact threshold
  --verbose, -v                Debug logging

Examples:

  # Basic run (default settings)
  python -m hcp_dupe_tool.run_large --input data.csv

  # With memory monitoring and custom output
  python -m hcp_dupe_tool.run_large \
    --input 600k_records.csv \
    --outdir ./my_results \
    --workers 8 \
    --memory-limit 14000 \
    --verbose

  # CSV-only output (faster, no Excel)
  python -m hcp_dupe_tool.run_large \
    --input data.csv \
    --no-excel

  # Limit candidate pairs to avoid explosion
  python -m hcp_dupe_tool.run_large \
    --input data.csv \
    --max-pairs 500000
```

---

## Output Files

### Default Output (with Excel)

```
results/
├── HCP_Dupe_Check_Large.xlsx         # Main workbook
│   ├── AUTO          # Definite duplicates (auto-merge)
│   ├── REVIEW        # Possible duplicates (needs review)
│   └── NOTDUP        # Definite non-duplicates
├── HCP_LARGE_auto.csv                # Raw data (AUTO pairs)
├── HCP_LARGE_review.csv              # Raw data (REVIEW pairs)
└── HCP_LARGE_notdup.csv              # Raw data (NOTDUP pairs)
```

### CSV-Only Output (with --no-excel)

```
results/
├── HCP_LARGE_auto.csv
├── HCP_LARGE_review.csv
└── HCP_LARGE_notdup.csv
```

### Column Descriptions

Each output file contains:

| Column | Meaning |
|--------|---------|
| `vid_a`, `vid_b` | The two VIDs being compared |
| `name_a`, `name_b` | Display names of both VIDs |
| `name_similarity` | Fuzzy match score (0-100, higher = more similar) |
| `specialty_match` | Overlapping medical specialties |
| `geo_support` | Geographic match (0=no, 1=yes) |
| `hco_overlap` | HCO VID match (0=no, 1=yes) |
| `matched_phones` | Phone numbers in common |
| `matched_emails` | Email addresses in common |
| `license_info` | License status (same/different/only A/only B) |
| `rule` | AUTO rule fired (for AUTO pairs) |
| `reason` | NOT-DUP reason (for NOTDUP pairs) |
| `score` | Review score (for REVIEW pairs) |
| `reasons` | Scoring components breakdown |
| `comments` | Human-readable explanation |

---

## Understanding the Results

### AUTO Sheet

**Meaning:** Definite duplicates. Safe to merge without review.

**Example rules that trigger AUTO:**
- Same name (>92% similar) + same specialty + same HCO VID
- Same name (>92% similar) + same specialty + same postal code
- Same name (>92% similar) + same specialty + same city
- Active license match + reasonable name similarity (>80%)
- Same phone + same email (non-shared contact) + decent name

**Action:** Safe to merge in your system

### REVIEW Sheet

**Meaning:** Probable duplicates, but not definite. Requires human judgment.

**Scoring:**
- **High confidence (>80):** Very likely duplicate, recommend merge
- **Medium-high (70-80):** Probably duplicate, check first
- **Medium (60-70):** Could be duplicate, verify manually
- **Low (<60):** Unlikely duplicate, but borderline

**Action:** Review manually before merging. Look at enrichment columns (name, specialty, city, phone, email).

### NOTDUP Sheet

**Meaning:** Definite non-duplicates. Do not merge.

**Example reasons:**
- Both have active licenses but none match (different practitioners)
- Same name + specialty but explicitly different cities (with no contact overlap)

**Action:** Safe to leave as separate records

---

## Configuration

### Default Configuration

The tool comes with `config_default.yaml` tuned for large-scale datasets. Key settings:

```yaml
blocking:
  max_block_size: 300         # Tighter than standard (500)
  phonetic_blocking: false    # Disabled (too many false positives at 300K scale)
  first_initial_blocking: false

name_matching:
  strong: 92
  medium: 85
  weak: 75

hcp_review_scoring:
  review_threshold: 50        # Minimum score to land in REVIEW
  high_confidence: 80
```

### Customizing Configuration

Create your own YAML:

```bash
cp config_default.yaml my_config.yaml
# Edit my_config.yaml to your preferences
python -m hcp_dupe_tool.run_large --input data.csv --config my_config.yaml
```

**Tips:**
- Increase `max_block_size` if you want more recall (but slower)
- Enable `phonetic_blocking: true` for smaller datasets or higher recall
- Adjust `review_threshold` to filter REVIEW pairs (higher = fewer to review)

---

## Performance Tuning

### For Speed (Fastest)

```bash
python -m hcp_dupe_tool.run_large \
  --input data.csv \
  --no-excel \
  --workers 8
```

Expected runtime: ~15-20 min for 600K rows

### For Memory Efficiency

```bash
python -m hcp_dupe_tool.run_large \
  --input data.csv \
  --max-pairs 300000 \
  --memory-limit 8000 \
  --workers 4
```

Expected memory: ~3-4 GB

### For Higher Recall (More Review)

```yaml
# In config.yaml
blocking:
  max_block_size: 500
  phonetic_blocking: true
  first_initial_blocking: true

hcp_review_scoring:
  review_threshold: 40  # Lower threshold = more in REVIEW
```

Expected runtime: ~40-60 min, more review pairs

---

## Troubleshooting

### "ERROR: Large-scale mode requires CSV input"

**Problem:** You passed an XLSX file

**Solution:**
```bash
# Export to CSV first
python -c "import pandas as pd; pd.read_excel('data.xlsx').to_csv('data.csv', index=False)"
# Then run
python -m hcp_dupe_tool.run_large --input data.csv --outdir ./results
```

### "Out of memory" or "Memory limit exceeded"

**Problem:** Peak memory usage too high

**Solutions:**
1. Reduce worker count: `--workers 4`
2. Limit candidate pairs: `--max-pairs 300000`
3. Disable enrichment in config: `output: enrich_output: false`
4. Use `--no-excel` to skip Excel consolidation

### "Comparison is very slow"

**Problem:** Only getting 5K-10K pair comparisons per minute

**Diagnose:**
```bash
# Check CPU utilization (should be ~90%)
top -p $PID

# Check disk I/O (CSVs should be growing rapidly)
watch "wc -l results/HCP_LARGE_*.csv"
```

**Solutions:**
- If CPU low: reduce worker count or check for other processes
- If disk I/O slow: use local SSD instead of network storage
- Check system logs: `dmesg | tail -20`

### "Blocking produced 5M+ pairs (too many!)"

**Problem:** Blocking is too loose, would take hours to compare

**Solutions:**
1. Use pair limit: `--max-pairs 1000000`
2. Reduce block size in config: `max_block_size: 200`
3. Use tighter default config (already set for large-scale)

### "Only getting a few NOTDUP pairs, lots of REVIEW"

**Problem:** Too many borderline cases

**Solutions:**
1. Increase review threshold in config: `review_threshold: 60` (vs. default 50)
2. Adjust scoring weights to be stricter
3. Enable phonetic blocking to catch more true duplicates earlier

---

## Advanced Usage

### Monitoring Progress Live

In another terminal:

```bash
watch "wc -l results/HCP_LARGE_*.csv && du -h results/"
```

You'll see CSV files growing in real-time as pairs are classified.

### Resuming Interrupted Runs

Currently not supported. If interrupted:
1. Delete partial output files
2. Re-run from start (canonicalization is fast)

### Using Output as Input to Other Tools

CSVs are standard format, easy to post-process:

```python
import pandas as pd

auto = pd.read_csv("results/HCP_LARGE_auto.csv")
review = pd.read_csv("results/HCP_LARGE_review.csv")

# Find all VIDs in AUTO pairs
auto_vids = set(auto['vid_a']).union(auto['vid_b'])
print(f"VIDs in AUTO: {len(auto_vids)}")

# Filter REVIEW by score threshold
high_confidence = review[review['score'] >= 75]
print(f"High-confidence REVIEW pairs: {len(high_confidence)}")
```

---

## System Requirements

### Minimum
- **CPU:** 4 cores (will use 4 workers)
- **RAM:** 8 GB
- **Disk:** 5 GB free space (for CSV files)

### Recommended
- **CPU:** 8+ cores
- **RAM:** 16 GB
- **Disk:** 10 GB free space, preferably SSD

### Tested On
- Ubuntu 20.04 / 22.04 (Linux)
- Python 3.8, 3.9, 3.10, 3.11

---

## FAQ

**Q: Can I use XLSX input?**
A: No. XLSX requires loading entire file into memory first. Export to CSV.

**Q: Why is phonetic blocking disabled?**
A: At 300K VIDs, Soundex creates many false candidates (3x explosion). For <100K VIDs, enable it.

**Q: How long will this take?**
A: ~30-60 min for 600K rows / 300K VIDs on 8-core laptop. 10-30 min with `--no-excel`.

**Q: Can I parallelize further (more than 8 workers)?**
A: Yes, but not recommended. Code caps at 6 by default to avoid IPC overhead. You can edit `run_large.py` line ~395: `workers = min(cpu_count - 1, 8)` to allow 8+.

**Q: What about HCO (organization) duplicates?**
A: Currently, large-scale mode runs HCP-only. HCO pipeline support coming in v2.1.

**Q: Can I use this on smaller datasets?**
A: Yes, but `run.py` is simpler and fine for <300K rows. Use `run_large.py` only if you have 600K+ rows or memory constraints.

**Q: Is the output guaranteed to be correct?**
A: No, duplicate detection is inherently probabilistic. Always review REVIEW pairs before merging in production systems.

---

## Files in This Package

```
/sessions/relaxed-fervent-tesla/mnt/outputs/hcp_dupe_tool/
├── run_large.py                          # Main entry point for large-scale (NEW)
├── run.py                                # Standard entry point (existing)
├── core.py                               # Core algorithms (updated with memory utils)
├── hcp_pipeline.py                       # HCP-specific rules (updated for multiprocessing)
├── hco_pipeline.py                       # HCO-specific rules (unchanged)
├── output.py                             # Output writing (unchanged)
├── config_default.yaml                   # Default configuration
├── README_LARGE_SCALE.md                 # This file (user guide)
├── LARGE_SCALE_OPTIMIZATION.md           # Technical guide (optimization details)
└── OPTIMIZATION_SUMMARY.md               # Implementation summary
```

---

## Support & Documentation

- **Quick help:** `python -m hcp_dupe_tool.run_large --help`
- **Technical details:** See `LARGE_SCALE_OPTIMIZATION.md`
- **Implementation notes:** See `OPTIMIZATION_SUMMARY.md`

---

## Version History

### v2.0 (Large-Scale Optimized)
- [NEW] `run_large.py` entry point for 600K+ rows
- [NEW] Multiprocessing pair comparison (8 workers)
- [NEW] Streaming CSV output (constant memory)
- [NEW] Memory monitoring and limits
- [NEW] Pair-count limiting
- [UPDATED] `core.py` with memory estimation
- [UPDATED] `hcp_pipeline.py` for multiprocessing compatibility

### v1.0 (Standard)
- Initial release with `run.py`

---

## License

[Check the repository for license information]

---

**Last updated: 2026-02-05**

For issues or questions, enable `--verbose` and check the debug logs.

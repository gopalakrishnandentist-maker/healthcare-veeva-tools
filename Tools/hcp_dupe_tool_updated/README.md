# HCP / HCO Duplicate Detection Tool

Rule-based duplicate detection for Veeva OpenData HCP and HCO records.
Designed for pharmaceutical data operations — scoring is transparent, auditable, and tuned for Indian Veeva extracts.

---

## What This Tool Does

| Mode | Input | Output |
|------|-------|--------|
| HCP dedup | A list of HCP VIDs | AUTO-MERGE / REVIEW / NOT-DUP pairs |
| HCO dedup (Run 1) | Target HCO parent VIDs (self-dedup) | Within-list duplicate pairs |
| HCO dedup (Run 2) | Target HCOs + universe HCOs (cross-check) | Target-vs-universe duplicate pairs |

All outputs are Excel workbooks with color-coded sheets and VID-safe formatting.

---

## Quick Start

### HCP Deduplication (Streamlit UI)
```bash
streamlit run app.py
```

### HCP Deduplication (CLI)
```bash
python run.py --input data.xlsx --outdir ./results
```

### HCP Deduplication (Large Scale — 600K+ rows)
```bash
python run_large.py --input data.csv --outdir ./results
```

### HCO Deduplication (scripted — see HCO_Dedup_Playbook.md)
```python
from hco_pipeline import run_hco_pipeline, run_hco_cross_pipeline
```

---

## File Structure

```
hcp_dupe_tool_updated/
├── hco_pipeline.py          ← HCO dedup engine (Run 1 + Run 2)
├── hcp_pipeline.py          ← HCP dedup engine
├── core.py                  ← Shared utilities (blocking, normalization, scoring)
├── run.py                   ← HCP CLI entry point (standard)
├── run_large.py             ← HCP CLI entry point (large-scale, multiprocessing)
├── app.py                   ← Streamlit UI
├── output.py                ← Excel output formatting
├── common_names.py          ← Common first name list for HCP matching
├── lookup.py                ← Reference data lookup
├── cross_match.py           ← Cross-dataset matching utilities
├── config_default.yaml      ← Default configuration
├── HCO_Dedup_Playbook.md    ← Full HCO project reuse guide (start here)
├── README.md                ← This file
├── CHANGES.txt              ← Detailed version history
└── README_LARGE_SCALE.md   ← Large-scale optimization guide
```

---

## HCO Pipeline — Architecture

```
hco_pipeline.py
  ├── _build_hco_canonical()       Normalize + create one canonical record per VID
  │     ├── norm_text()            Lowercase, strip punctuation, collapse spaces
  │     ├── split " - " FIRST      Strip Veeva locality suffix BEFORE normalizing (Fix C)
  │     └── name_stripped_norm     Pre-computed locality-stripped normalized name
  ├── _compute_hco_signals()       Score a candidate pair
  │     ├── address overlap        Token overlap (≥70% gate, mandatory)
  │     ├── name similarity        Fuzzy match on locality-stripped name
  │     ├── acronym detection      is_acronym_of() — handles GSV→Ganesh Siddha Venkateswara
  │     ├── phone / fax / postal   Exact match signals
  │     └── type / city / state    Confirmation signals + penalties
  ├── _apply_hco_auto_rules()      H1–H5 rules for AUTO-MERGE classification
  ├── _apply_hco_review_scoring()  Point-based scoring for REVIEW classification
  ├── run_hco_pipeline()           Run 1: within-list self-dedup
  └── run_hco_cross_pipeline()     Run 2: target list vs universe cross-check
```

---

## HCO Scoring Weights

| Signal | Points | Notes |
|--------|--------|-------|
| address_match (≥70% token overlap) | +50 | **Mandatory gate** — pair skipped if address doesn't overlap |
| postal_match (exact) | +30 | Very strong signal |
| phone_match (exact) | +25 | Very strong signal |
| name_strong (≥92% similarity) | +25 | Applied after locality strip |
| fax_match (exact) | +15 | |
| name_medium (≥85%) | +15 | |
| type_match (exact) | +10 | |
| city_match | +10 | Already implied by address gate |
| acronym_match | +20 | e.g., AIIMS = All India Institute of Medical Sciences |
| name_weak (≥75%) | +8 | |
| state_match | +5 | |
| different_type | −15 | Penalty |
| different_city | −30 | Heavy penalty — city mismatch = wrong HCO |

**Thresholds:**
- Review threshold: 70 points
- Medium confidence: 75 points
- High confidence (AUTO): 90 points

---

## HCO AUTO Rules (H1–H5)

| Rule | Condition | Action |
|------|-----------|--------|
| H1 | Name ≥90% + Address overlap + Phone match | AUTO-MERGE |
| H2 | Name ≥92% + Address overlap + Type match | AUTO-MERGE |
| H3 | Name ≥90% + Phone match + Type match | AUTO-MERGE |
| H4 | Acronym match + Address overlap + Type match | AUTO-MERGE |
| H5 | Acronym match + Address overlap + Phone/Fax match | AUTO-MERGE |

---

## Veeva Data Conventions (India)

### HCO Naming
- `"HCO Name - Locality"` → standalone parent HCO
- `"HCO Name / Dept of XYZ"` → department record (child of a parent)

### Key Column Names
```
hco.vid__v (NETWORK ID)
hco.corporate_name__v (CORPORATE NAME)
hco.hco_type__v (HCO TYPE)
hco.hco_status__v (STATUS)
hco.business_type__v (TYPE OF BUSINESS)
address.address_line_1__v (ADDRESS LINE 1)
address.locality__v (CITY)
address.administrative_area__v (STATE/PROVINCE)
address.postal_code__v (ZIP/POSTAL CODE)
address.phone_1__v (PHONE 1)
address.fax_1__v (FAX 1)
address.address_ordinal__v (ADDRESS RANK)    ← lowest = primary/canonical
parenthco.parent_hco_vid__v                  ← parent VID of a dept record
```

### VID Safety
VIDs are 18-digit numbers — always keep as strings. Never cast to int or float.
Excel requires `number_format = "@"` on VID columns to prevent scientific notation corruption.

---

## Configuration (config_default.yaml)

Key HCO settings:
```yaml
hco_review_scoring:
  address_match: 50
  postal_match: 30
  phone_match: 25
  name_strong: 25
  review_threshold: 70
  high_confidence: 90

blocking:
  max_block_size: 500
  phonetic_blocking: true
```

See `HCO_Dedup_Playbook.md` for the full recommended CFG dict.

---

## Known Issues Fixed

### Fix C — Locality Strip (2026-03-19)
**Problem:** Veeva India names follow `"HCO Name - Locality"`. `norm_text()` converts ` - ` to a space before `_strip_locality()` can detect it, causing the city to bake into the name. Result: "Clinic - Malda" vs "Clinic - Malda" scored 76%+ similarity even with different clinic names.

**Fix:** Split on ` - ` BEFORE calling `norm_text()`. Store as `name_stripped_norm` in canonical record.

**Impact:** Run2 REVIEW false positives reduced by 94% (33,149 → 2,037 on Sanofi BC dataset).

---

## Output Style (Excel)

| Sheet Type | Header Color |
|-----------|-------------|
| Summary / Executive | `#1F4E79` (dark blue) |
| AUTO-MERGE results | `#375623` (dark green) |
| REVIEW results | `#BF8F00` (amber) |
| Findings / Risk | `#7B2C2C` (dark red) |

All VID columns: text format (`@`) to prevent Excel scientific notation corruption.

---

## Validated On

| Project | Records | Result |
|---------|---------|--------|
| Sanofi BC HCO (India) | 19,164 parent HCOs | Run1: 111 AUTO + 106 REVIEW; Run2: 1,748 AUTO + 2,037 REVIEW |

---

## Version History

See `CHANGES.txt` for detailed change log.

| Version | Date | Summary |
|---------|------|---------|
| v3.0 | 2026-03-19 | HCO pipeline enhancements: locality strip fix, acronym detection, Indian stop words, address-priority scoring, playbook |
| v2.0 | 2026-02-05 | Large-scale optimization: multiprocessing, CSV streaming, memory monitoring |
| v1.0 | Earlier | Initial release |

---

## For Full HCO Project Reuse

See **`HCO_Dedup_Playbook.md`** — a step-by-step guide covering:
- Data inspection and dept→parent resolution
- Enrichment from Master Data
- Run 1 and Run 2 configuration
- Business type and HCP affiliation enrichment
- All known bugs and their fixes
- Output standards and file naming

# HCO Duplicate Detection — Reusable Playbook
**Tool:** `hcp_dupe_tool_updated` (custom rule-based engine)
**Validated on:** Sanofi BC HCO project, India, ~19K parent HCOs, March 2026
**Author:** Gopalakrishnan KB

---

## 1. When to Use This Playbook

Use this playbook whenever you receive:
- A **target list** of HCOs (pharma client's covered accounts) from Veeva OpenData
- A **master data export** from Veeva (universe of HCO records)
- A task to find duplicates within the target list, or between target and master

---

## 2. File Inventory — What to Expect

| File Type | Typical Name Pattern | Key Column |
|-----------|---------------------|------------|
| Target File | `HCOs covered by [Team] (1).xlsx` | `Sanofi ID` (VID), `HCO Name` |
| Master Data | `Data Warehouse Report_YYYY-MM-DD.xlsx` | `hco.vid__v (NETWORK ID)` |
| Universe Master | `universe master.csv` | `hco.vid__v (NETWORK ID)` |
| Master HCP Data | `master hcp data.csv` | `hcp.vid__v`, `hco.parent_hco_vid__v`, `hco.grandparent_hco_vid__v`, `hcp.specialty_1__v` |
| Parent VID Mapping | (generated) `Sanofi_HCO_BC_Parent_VID_Mapping.xlsx` | `Parent_HCO_VID`, `Record_Type` |

---

## 3. Step-by-Step Workflow

### Step 1 — Inspect Target File

```python
tf = pd.read_excel(TARGET_FILE, dtype=str)
dept_rows   = tf[tf["HCO Name"].str.contains("/", na=False)]
parent_rows = tf[~tf["HCO Name"].str.contains("/", na=False)]
print(f"Total: {len(tf)} | Depts: {len(dept_rows)} | Parents: {len(parent_rows)}")
assert len(dept_rows) + len(parent_rows) == len(tf)
```

**Veeva naming convention:**
- `"HCO Name - Locality"` → standalone parent HCO
- `"HCO Name / Dept of XYZ"` → department record (child of a parent HCO)

**Watch for edge cases:** Some HCOs have `/` in their actual business name (e.g., `"M/S - Phulo Medical"`). These will be misclassified as depts. Flag them manually.

---

### Step 2 — Resolve Dept Records to Parent VIDs

Department records are NOT the entity to dedup — their parent HCO is. Resolve via `parenthco.parent_hco_vid__v` in Master Data:

```python
md = pd.read_excel(MASTER_FILE, sheet_name="Sheet1", dtype=str)

dept_vids = set(dept_rows["Sanofi ID"].dropna())
parent_lookup = md[md["hco.vid__v (NETWORK ID)"].isin(dept_vids)][
    ["hco.vid__v (NETWORK ID)", "parenthco.parent_hco_vid__v"]
].drop_duplicates()

# Build mapping output
# Record_Type: "Parent HCO (Target)" vs "Dept (Target)"
# Parent_HCO_VID: direct VID for parent rows
# Parent_HCO_VID_Derived: looked-up parent VID for dept rows
```

**Expected outcome:** ~4 dept VIDs will have no parent in Master Data. These are typically:
- HCOs whose name contains `/` as part of the business name (not a dept separator)
- Park and investigate, do not drop

**Combine unique parent VIDs:**
```python
direct_parents  = set(mapping[mapping["Record_Type"] == "Parent HCO (Target)"]["Parent_HCO_VID"].dropna())
derived_parents = set(mapping[mapping["Record_Type"] == "Dept (Target)"]["Parent_HCO_VID_Derived"].dropna())
parent_vids = direct_parents | derived_parents
# Log overlap: len(direct_parents & derived_parents) — these are HCOs that appear
# both as standalone rows AND as parents of dept records (valid, not an error)
```

---

### Step 3 — Enrich Target Parents from Master Data

```python
md_enriched = md[md["hco.vid__v (NETWORK ID)"].isin(parent_vids)].copy()

# Canonical row = lowest address ordinal (primary address)
md_enriched = (
    md_enriched
    .sort_values(["hco.vid__v (NETWORK ID)", "address.address_ordinal__v (ADDRESS RANK)"])
    .drop_duplicates(subset=["hco.vid__v (NETWORK ID)"], keep="first")
)

matched   = md_enriched["hco.vid__v (NETWORK ID)"].nunique()
unmatched = len(parent_vids) - matched
print(f"Matched: {matched} | Unmatched: {unmatched}")
```

---

### Step 4 — Run 1: Within-Target Dedup

```python
from hco_pipeline import run_hco_pipeline

within_results = run_hco_pipeline(md_enriched, CFG, progress_fn=lambda pct, msg: print(msg))

w_auto   = within_results.get("hco_auto",      pd.DataFrame())
w_review = within_results.get("hco_review",    pd.DataFrame())
w_notdup = within_results.get("hco_notdup",    pd.DataFrame())
```

---

### Step 5 — Run 2: Target vs Universe Cross-Check

```python
from hco_pipeline import run_hco_cross_pipeline

DEPT_TYPE = "Organization, Dept at Hospital"
univ_parents = (
    univ_raw[univ_raw["hco.hco_type__v (HCO TYPE)"] != DEPT_TYPE]
    .pipe(lambda df: df[~df["hco.vid__v (NETWORK ID)"].isin(parent_vids)])  # exclude targets
    .sort_values(["hco.vid__v (NETWORK ID)", "address.address_ordinal__v (ADDRESS RANK)"])
    .drop_duplicates(subset=["hco.vid__v (NETWORK ID)"], keep="first")
)

target_canon = within_results.get("hco_canonical", pd.DataFrame())
cross_results = run_hco_cross_pipeline(target_canon, univ_parents, CFG, progress_fn=print)

c_auto   = cross_results.get("cross_auto",   pd.DataFrame())
c_review = cross_results.get("cross_review", pd.DataFrame())
```

---

### Step 6 — Enrich Output with Business Type

```python
# Universe master is the authoritative source for business type
univ = pd.read_csv(UNIVERSE_FILE, dtype=str)
bt_map = (
    univ.drop_duplicates(subset=["hco.vid__v (NETWORK ID)"])
    .set_index("hco.vid__v (NETWORK ID)")["hco.business_type__v (TYPE OF BUSINESS)"]
)

# If a client provides a manual override sheet (e.g., "Dr M sheet.csv"):
# Rule: if VID is in override sheet → use override classification; else keep universe value
override = pd.read_csv(OVERRIDE_FILE, dtype=str)
override_map = override.set_index("hco.vid__v (NETWORK ID)")["HCO business type"]

def get_bt(vid):
    if vid in override_map.index:
        return override_map[vid]
    return bt_map.get(vid, "Unknown")
```

---

### Step 7 — HCP Affiliation Count

```python
hcp = pd.read_csv(HCP_FILE, dtype=str)

P_COL = "hco.parent_hco_vid__v (PARENT_HCO_VID__V)"
G_COL = "hco.grandparent_hco_vid__v (GRANDPARENT_HCO_VID__V)"
S_COL = "hcp.specialty_1__v (SPECIALTY 1)"
H_COL = "hcp.vid__v (NETWORK ID)"

# Filter to HCPs affiliated to any target HCO (via parent or grandparent link)
mask = hcp[P_COL].isin(target_vids) | hcp[G_COL].isin(target_vids)
hcp_filtered = hcp[mask].copy()

# Assign matched target VID (parent takes priority over grandparent)
hcp_filtered["matched_vid"] = hcp_filtered[P_COL].where(
    hcp_filtered[P_COL].isin(target_vids),
    hcp_filtered[G_COL]
)

# Dedup per (matched_vid, hcp_vid) — prevents double-counting HCPs in both routes
hcp_filtered = hcp_filtered.drop_duplicates(subset=["matched_vid", H_COL])

# Aggregate
SPEC_MAP = {
    "General_Practice":             "General Practice",
    "General_Medicine":             "General Medicine",
    "Cardiovascular_Disease":       "Cardiovascular Disease",
    "Endocrinology_Diabetes":       "Endocrinology, Diabetes, & Metabolism",
    "Nephrology":                   "Nephrology",
}

def agg_hcps(group):
    total = len(group)
    row = {"Total_HCPs": total}
    for col_name, spec in SPEC_MAP.items():
        row[col_name] = (group[S_COL] == spec).sum()
    row["Other_Specialties"] = total - sum(row[s] for s in SPEC_MAP)
    return pd.Series(row)

hcp_agg = hcp_filtered.groupby("matched_vid").apply(agg_hcps).reset_index()
hcp_agg.rename(columns={"matched_vid": "hco_vid"}, inplace=True)
```

**Validation checkpoints:**
- `19,157 + 7 (no HCPs) = 19,164` total target VIDs ✓
- `sum(specialty cols) == Total_HCPs` for every row ✓
- Grand total unique HCPs = `hcp_filtered[H_COL].nunique()`
- Grand total HCP-HCO links = `len(hcp_filtered)` (different from unique HCPs)

---

### Step 8 — Final Report Structure

See Section 6 below for the standard column layout and sheet structure.

---

## 4. CFG — Recommended Configuration

```python
CFG = {
    "columns": {
        "hco_entity_vid":  "hco.vid__v (NETWORK ID)",
        "hco_name":        "hco.corporate_name__v (CORPORATE NAME)",
        "hco_type":        "hco.hco_type__v (HCO TYPE)",
        "hco_phone":       "address.phone_1__v (PHONE 1)",
        "hco_fax":         "address.fax_1__v (FAX 1)",
        "hco_city":        "address.locality__v (CITY)",
        "hco_state":       "address.administrative_area__v (STATE/PROVINCE)",
        "hco_postal":      "address.postal_code__v (ZIP/POSTAL CODE)",
        "hco_addr_line1":  "address.address_line_1__v (ADDRESS LINE 1)",
        "hco_status":      "hco.hco_status__v (STATUS)",
    },
    "blocking": {
        "max_block_size":    500,
        "phonetic_blocking": True,
    },
    "hco_auto_rules": {
        "H1_name_addr_phone": {"name_min": 90},
        "H2_name_addr_type":  {"name_min": 92},
        "H3_name_phone_type": {"name_min": 90},
        # H4 (acronym+addr+type) and H5 (acronym+addr+phone) handled in _apply_hco_auto_rules
    },
    "hco_review_scoring": {
        "address_match":  50,   # primary gate + highest scorer
        "postal_match":   30,
        "city_match":     10,
        "name_strong":    25,   # ≥92% stripped name
        "name_medium":    15,   # ≥85%
        "name_weak":       8,   # ≥75%
        "acronym_match":  20,
        "phone_match":    25,
        "fax_match":      15,
        "type_match":     10,
        "state_match":     5,
        "different_type": -15,
        "different_city": -30,
        "review_threshold": 70,
        "high_confidence":  90,
        "medium":           75,
    },
    "manual_review": {"pairs_per_hour": 6.5},
    "output":        {"enrich_output": True},
    "profile_type":  "hco",
}
```

---

## 5. Critical Bugs & Fixes (learn from these)

### Bug A — Locality Strip Never Fires (Fix C)
**Symptom:** Run2 REVIEW shows 33K+ pairs instead of ~2K. Pairs like "Dr M Rashid's Clinic - Malda" vs "Dr B K Ghosh's Clinic - Malda" score 76%+ name similarity (should score ~47%).

**Root cause:** `norm_text()` is called before `_strip_locality()`. `norm_text()` converts ` - ` to a space, so `_strip_locality()` never finds the ` - ` separator. The city (Malda) gets baked into the normalized name as the last token.

**Fix in `_build_hco_canonical`:**
```python
# Split on " - " BEFORE normalizing
raw_name_for_cmp = raw_name.split(" - ")[0].strip() if " - " in raw_name else raw_name
name_stripped_norm = norm_text(raw_name_for_cmp)  # store this in canonical record
```

**Fix in `_compute_hco_signals`:**
```python
# Use pre-computed stripped name instead of stripping after norm
a_name_stripped = ar.get("name_stripped_norm") or _strip_locality(ar["name_norm"])
b_name_stripped = br.get("name_stripped_norm") or _strip_locality(br["name_norm"])
```

**Impact:** −94% false positives in Run2 REVIEW.

---

### Bug B — Probabilistic Model on Low-Dupe Data
**Symptom:** splink generates 17K–123K AUTO pairs from 24K records.

**Root cause:** EM algorithm needs enough genuine duplicates to calibrate m-probabilities. Veeva data has <1% dupe rate — the model can't distinguish signal from noise.

**Fix:** Abandon probabilistic approach. Use rule-based engine with explainable thresholds.

---

### Bug C — Dept Sibling False Positives
**Symptom:** "Apollo Hospital / Dept of Nephrology" flagged as duplicate of "Apollo Hospital / Dept of Cardiology".

**Root cause:** Same parent name, same address, same parent HCO — name similarity is very high.

**Fix:** Never include dept records in dedup input. Always resolve to parent VID first, then dedup parent VIDs only.

---

### Bug D — Specialty Name Mismatch (zero results)
**Symptom:** Specialty column shows 0 for all HCOs for certain specialties.

**Root cause:** Exact string mismatch between template and actual Veeva values.

**Correct Veeva specialty strings (India):**
- `"Cardiovascular Disease"` (NOT "Cardiovascular **Diseases**")
- `"Endocrinology, Diabetes, & Metabolism"` (NOT "Endocrinology/Diabetes")
- `"General Practice"`, `"General Medicine"`, `"Nephrology"` — these match as-is

**Fix:** Always verify specialty strings by `hcp["specialty_col"].value_counts()` before hardcoding.

---

## 6. Output Standards

### Header Colors
| Sheet Type | Color | Hex |
|-----------|-------|-----|
| Summary / Executive | Dark blue | `1F4E79` |
| AUTO-MERGE | Dark green | `375623` |
| REVIEW | Amber | `BF8F00` |
| Findings / Risk | Dark red | `7B2C2C` |

### Standard Report Sheets (Final Report)
1. Executive Summary
2. HCO Duplicate Summary
3. Run1 - AUTO (within target)
4. Run1 - REVIEW (within target)
5. Run2 - AUTO (target vs universe)
6. Run2 - REVIEW (target vs universe)
7. Findings

### Enhanced Run Sheet Column Layout
```
VID_A | HCO_Name_A | Business_Type_A | Total_HCPs_A |
  General_Practice_A | General_Medicine_A | Cardiovascular_Disease_A |
  Endocrinology_Diabetes_A | Nephrology_A | Other_Specialties_A |
VID_B | HCO_Name_B | Business_Type_B | Total_HCPs_B |
  (same specialty columns for B)
```

For Run2 (target vs master), prefix with `Target_` and `Master_` instead of `_A` and `_B`.

### HCO HCP Affiliation Count — Sheets
1. Summary — one row per target HCO + grand total row
2. Detail — full HCP-level data (large file)
3. No HCP Affiliation — HCOs with zero HCP links + their details
4. Public HCOs — same breakdown filtered to business_type = "Public"

---

## 7. Key Metrics to Always Report

### Input validation:
- Total target records | dept records | parent records (cross-check sum)
- Parent VIDs: direct + derived + overlap + combined unique
- Parent VIDs found in Master Data | missing

### Run 1:
- AUTO pairs | REVIEW pairs | NOT-DUP pairs
- Candidate pairs evaluated (= sum of all three)

### Run 2:
- Universe records (excl. target + dept)
- Cross AUTO pairs | Cross REVIEW pairs

### HCP:
- Unique HCP-HCO links (Grand Total links)
- Globally unique HCPs (deduplicated across all HCOs)
- HCOs with ≥1 HCP | HCOs with 0 HCPs
- Public HCO HCP share (Public links / Total links)

### Dedup grand totals:
- Total flagged VIDs | AUTO only | REVIEW only | Both AUTO and REVIEW
- Net unique VIDs (total − flagged)

---

## 8. File Naming Convention

```
Sanofi_HCO_BC_Parent_VID_Mapping.xlsx       — Step 2 output
Sanofi_HCO_BC_Dupe_Results_YYYY-MM-DD.xlsx  — Step 4/5 output (5 sheets)
HCO_HCP_Affiliation_Count.xlsx              — Step 7 output (4 sheets)
Sanofi_HCO_BC_Final_Report.xlsx             — Final delivery (7 sheets)
```

Store all final deliverables in a `Final/` subfolder.

---

## 9. Reuse for Other Clients / Projects

To adapt this playbook for a new client:
1. Update `TARGET_FILE`, `MASTER_FILE`, `UNIVERSE_FILE`, `HCP_FILE` paths
2. Verify column names match (check first 5 rows of each file)
3. Verify specialty strings via `value_counts()` on specialty column
4. Check HCO naming convention — confirm `/` = dept separator for this client
5. Run a small test (100 VIDs) before full pipeline
6. Validate grand totals before delivering output

The CFG scoring weights and thresholds in Section 4 are calibrated for India Veeva data. For other geographies, the address stop words and locality strip logic may need adjustment.

---

## 10. Tool Architecture (quick reference)

```
hco_pipeline.py
  ├── _build_hco_canonical()     — normalise + create canonical record per VID
  │     └── name_stripped_norm   — locality-stripped name (split BEFORE norm_text)
  ├── _compute_hco_signals()     — score a pair (address, name, phone, type, etc.)
  ├── _apply_hco_auto_rules()    — H1–H5 rules for AUTO-MERGE
  ├── run_hco_pipeline()         — self-dedup (Run 1)
  └── run_hco_cross_pipeline()   — cross-dedup (Run 2)
```

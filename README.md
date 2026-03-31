# Healthcare Data Management Toolkit — Veeva OpenData

**Production-grade tools for HCP/HCO data operations in pharmaceutical life sciences.**

A suite of Python tools built for Veeva OpenData operations supporting pharmaceutical clients. Handles duplicate detection, data validation, VID integrity protection, and change request processing across datasets of 600K+ HCPs and 250K+ HCOs.

---

## Tools

| Tool | Description | Interface |
|------|-------------|-----------|
| **HCP/HCO Duplicate Detection Engine** | Rule-based duplicate detection with transparent scoring, acronym matching, and India-specific address normalization. Produces auditable AUTO-MERGE and REVIEW pair lists. | CLI + Streamlit |
| **HCP Data Validator** | Validates HCP records across four checks: license status, candidate flags, active HCO affiliations, and HCP status. Handles 150K+ row datasets. | Streamlit GUI |
| **VID Data Shield** | Prevents Excel scientific notation corruption of 18-digit Veeva identifiers during file conversion workflows. | Streamlit |
| **PDR Processor** | Triages Proactive Data Change Requests by parsing free-text notes, fuzzy-matching against network data, and classifying into confidence tiers. | CLI + Streamlit |
| **DCR Tracker** | Tracks Data Change Request lifecycle and status updates. | CLI |

---

## Key Capabilities

- **Rule-based deduplication** -- Transparent, auditable scoring designed for Veeva data where duplicate rates are below 1%. Probabilistic tools fail at this scale; rule-based decisions are explainable to clients.
- **VID-safe data handling** -- All 18-digit Veeva identifiers treated as strings at every stage. Excel output enforces text formatting to prevent silent data corruption.
- **India-specific optimizations** -- Locality stripping for Veeva's `"HCO Name - City"` convention, Indian address stop words, acronym detection for institutional names (AIIMS, JIPMER, etc.), state medical council mappings.
- **3-layer audit protocol** -- Source traceability, computational verification, and cross-reference integrity checks built into every pipeline. Independent recheck pass before any client delivery.
- **Color-coded Excel reporting** -- Standardized output with dark blue summaries, green AUTO-MERGE sheets, amber REVIEW sheets, and red findings. All VID columns formatted as text.

---

## Duplicate Detection Architecture

```
Input: Veeva HCO/HCP extract (CSV/Excel)
  |
  v
Canonicalization
  ├── Strip locality suffix (" - City") BEFORE normalization
  ├── Normalize text (lowercase, collapse whitespace, strip punctuation)
  └── Build one canonical record per VID (lowest address ordinal)
  |
  v
Blocking (phonetic + postal code)
  |
  v
Pairwise Scoring
  ├── Address token overlap (mandatory gate, >= 70%)
  ├── Name similarity (fuzzy match on locality-stripped name)
  ├── Acronym detection (GSV = Ganesh Siddha Venkateswara)
  ├── Phone / Fax / Postal exact match
  └── Type / City / State confirmation + penalties
  |
  v
Classification
  ├── AUTO-MERGE: score >= 90 or deterministic rules (H1-H5)
  ├── REVIEW: score >= 70
  └── NOT-DUP: below threshold
  |
  v
Output: Styled Excel workbook with paired records and evidence
```

---

## Repository Structure

```
Tools/
├── hcp_dupe_tool_updated/     # Latest HCP/HCO dedup engine (v3.0)
│   ├── hco_pipeline.py        # HCO dedup: Run 1 (self-dedup) + Run 2 (cross-check)
│   ├── hcp_pipeline.py        # HCP dedup engine
│   ├── core.py                # Blocking, normalization, scoring
│   ├── app.py                 # Streamlit UI
│   ├── run.py / run_large.py  # CLI entry points
│   └── HCO_Dedup_Playbook.md  # Full project reuse guide
├── HCP_Validator_v2/          # HCP data quality validator (v2.0)
├── vid_cleaner/               # VID Data Shield
├── pdr processor/             # PDR triage dashboard
└── Archive/                   # Earlier tool versions

Prompts/                       # AI prompt templates for data operations
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Processing | pandas, numpy |
| Fuzzy Matching | rapidfuzz, difflib |
| Excel I/O | openpyxl, xlsxwriter |
| UI | Streamlit |
| Visualization | Plotly, matplotlib |
| Language | Python 3.10+ |

---

## Validated On

| Client Dataset | Records | Result |
|---------------|---------|--------|
| HCO Dedup (India) | 19,164 parent HCOs | Run 1: 111 AUTO + 106 REVIEW; Run 2: 1,748 AUTO + 2,037 REVIEW |
| HCP Validation | 150K+ HCP records | License, affiliation, candidate, and status checks |
| VID Protection | 800MB+ exports | Zero corruption on 18-digit identifiers |

---

## Built For

Veeva OpenData India Operations — supporting HCP/HCO data quality for pharmaceutical clients where accuracy is non-negotiable and every number faces director-level review.

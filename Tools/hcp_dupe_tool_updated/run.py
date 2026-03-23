#!/usr/bin/env python3
"""
run.py — CLI entry point for the HCP/HCO Duplicate Identification Tool.

Usage examples:

  # Minimal (uses default config, auto-detects sheet)
  python -m hcp_dupe_tool.run --input raw.xlsx --outdir ./out

  # With custom config and specific sheet
  python -m hcp_dupe_tool.run --input raw.xlsx --sheet Sheet0 --config my_config.yaml --outdir ./out

  # CSV input
  python -m hcp_dupe_tool.run --input raw.csv --outdir ./out

  # HCP only (skip HCO pipeline)
  python -m hcp_dupe_tool.run --input raw.xlsx --outdir ./out --hcp-only

  # Override shared-contact threshold via CLI
  python -m hcp_dupe_tool.run --input raw.xlsx --outdir ./out --shared-threshold 10
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

import pandas as pd

# ── Config loading ───────────────────────────────────────────────────

def _load_config(config_path: str | None) -> dict[str, Any]:
    """Load YAML config, falling back to built-in defaults."""

    defaults_path = os.path.join(os.path.dirname(__file__), "config_default.yaml")

    try:
        import yaml
    except ImportError:
        print(
            "WARNING: PyYAML not installed — using hardcoded defaults.\n"
            "  Install with:  pip install pyyaml\n"
        )
        return _hardcoded_defaults()

    # Load defaults first
    cfg: dict[str, Any] = {}
    if os.path.isfile(defaults_path):
        with open(defaults_path, "r") as f:
            cfg = yaml.safe_load(f) or {}

    # Overlay user config
    if config_path and os.path.isfile(config_path):
        with open(config_path, "r") as f:
            user_cfg = yaml.safe_load(f) or {}
        cfg = _deep_merge(cfg, user_cfg)
        print(f"Loaded config: {config_path}")
    elif config_path:
        print(f"WARNING: Config file not found: {config_path} — using defaults.")

    return cfg


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge overlay into base dict."""
    merged = base.copy()
    for k, v in overlay.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _hardcoded_defaults() -> dict[str, Any]:
    """Minimal hardcoded defaults when YAML isn't available."""
    return {
        "columns": {
            "hcp_vid": "hcp.vid__v (VID)",
            "hcp_last_name": "hcp.last_name__v (LAST NAME)",
            "hcp_first_name": "hcp.first_name__v (FIRST NAME)",
            "hcp_middle_name": "hcp.middle_name__v (MIDDLE NAME)",
            "hcp_full_name": "hcp.source_full_name__v (SOURCE FULL NAME)",
            "specialty_1": "hcp.specialty_1__v (SPECIALTY 1)",
            "specialty_2": "hcp.specialty_2__v (SPECIALTY 2)",
            "specialty_3": "hcp.specialty_3__v (SPECIALTY 3)",
            "specialty_4": "hcp.specialty_4__v (SPECIALTY 4)",
            "license_number": "license.license_number__v (LICENSE)",
            "license_body": "license.licensing_body__v (LICENSING BODY)",
            "license_status": "license.license_status__v (LICENSE STATUS)",
            "hco_vid": "hco.hco_vid__v (HCO_VID__V)",
            "parent_hco_vid": "hco.parent_hco_vid__v (PARENT_HCO_VID__V)",
            "grandparent_hco_vid": "hco.grandparent_hco_vid__v (GRANDPARENT_HCO_VID__V)",
            "city_cda": "hcp.city_cda__v (CITY (CDA))",
            "addr_city": "address.locality__v (CITY)",
            "addr_state": "address.administrative_area__v (STATE/PROVINCE)",
            "addr_postal": "address.postal_code__v (ZIP/POSTAL CODE)",
            "hcp_status": "hcp.hcp_status__v (STATUS)",
            "candidate_record": "hcp.candidate_record__v (CANDIDATE RECORD)",
            "phone_pattern": "hcp.phone_*",
            "email_pattern": "hcp.email_*",
            "hco_entity_vid": "hco.vid__v (VID)",
            "hco_name": "hco.primary_name__v (NAME)",
            "hco_type": "hco.hco_type__v (TYPE)",
            "hco_phone": "hco.phone__v (PHONE)",
            "hco_fax": "hco.fax__v (FAX)",
            "hco_city": "hco.city__v (CITY)",
            "hco_state": "hco.state__v (STATE)",
            "hco_postal": "hco.postal_code__v (POSTAL CODE)",
            "hco_addr_line1": "hco.address_line_1__v (ADDRESS LINE 1)",
            "hco_addr_line2": "hco.address_line_2__v (ADDRESS LINE 2)",
            "hco_status": "hco.hco_status__v (STATUS)",
        },
        "shared_contact": {"threshold": 5},
        "blocking": {
            "max_block_size": 500,
            "phonetic_blocking": True,
            "first_initial_blocking": True,
        },
        "name_matching": {
            "strip_suffixes": ["md", "do", "phd", "mbbs", "bds", "mds", "ms", "dr", "jr", "sr", "ii", "iii", "iv"],
            "strong": 92,
            "medium": 85,
            "weak": 75,
        },
        "hcp_auto_rules": {
            "G1_name_spec_hco": {"name_min": 92},
            "G2_name_spec_pin": {"name_min": 92},
            "G3_name_spec_city": {"name_min": 92},
            "G4_license_match": {"name_min": 80},
            "G5_phone_email": {"name_min": 85},
            "G6_email_name": {"name_min": 92},
        },
        "hcp_not_dup_rules": {
            "N1_active_license_conflict": {},
            "N2_different_cities": {"name_min": 92},
        },
        "hcp_review_scoring": {
            "name_strong": 35, "name_medium": 25, "name_weak": 15,
            "specialty_match": 30, "hco_overlap": 25, "pin_match": 25,
            "city_match": 20, "state_match": 15, "geo_support": 10, "license_match": 40,
            "phone_email_match": 20, "email_match": 15, "phone_match": 10,
            "specialty_conflict": -15, "different_cities": -20, "candidate_record": -10,
            "review_threshold": 50, "high_confidence": 80, "medium_high": 70, "medium": 60,
        },
        "hco_auto_rules": {
            "H1_name_addr_phone": {"name_min": 90},
            "H2_name_addr_type": {"name_min": 92},
            "H3_name_phone_type": {"name_min": 90},
        },
        "hco_review_scoring": {
            "name_strong": 35, "name_medium": 25, "name_weak": 15,
            "address_match": 30, "city_match": 20, "postal_match": 25,
            "phone_match": 20, "fax_match": 15, "type_match": 15, "state_match": 10,
            "different_type": -20, "different_city": -15,
            "review_threshold": 50, "high_confidence": 80, "medium": 60,
        },
        "manual_review": {
            "pairs_per_hour": 6.5,
        },
        "output": {
            "enrich_output": True,
            "write_csv": True,
            "max_contact_display": 200,
        },
        "profile_type": "auto",
        "profile_types": {
            "hcp": {
                "description": "Healthcare Professional",
            },
            "hco": {
                "description": "Healthcare Organization (Hospital, Clinic)",
                "strip_prefixes": [],
                "strip_suffixes": [],
            },
            "stockist": {
                "description": "Pharmaceutical Stockist / Distributor",
                "strip_prefixes": [
                    "m s", "ms", "messrs", "shri", "smt", "sri", "mr", "mrs",
                ],
                "strip_suffixes": [
                    "medical agency", "medical agencies", "medical store", "medical stores",
                    "medical hall", "pharma", "pharma distributors", "pharmaceutical",
                    "pharmaceuticals", "distributors", "distributor", "agencies", "agency",
                    "enterprises", "enterprise", "sales", "sales centre", "sales center",
                    "traders", "trading", "trading co", "company", "co",
                    "pvt ltd", "pvt", "ltd", "private limited", "limited",
                ],
                "hco_auto_rule_overrides": {
                    "H1_name_addr_phone": {"name_min": 93},
                    "H2_name_addr_type": {"name_min": 95},
                    "H3_name_phone_type": {"name_min": 93},
                },
                "review_scoring_overrides": {},
                "auto_detect_types": [
                    "pharmacy, retail", "pharmacy retail", "distributor",
                    "stockist", "wholesale", "wholesaler",
                    "c&f agent", "c and f", "clearing and forwarding",
                ],
            },
        },
    }


# ── Data Loading ─────────────────────────────────────────────────────

def _load_data(input_path: str, sheet: str | None) -> pd.DataFrame:
    """Load input file (Excel or CSV)."""
    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".csv":
        print(f"Reading CSV: {input_path}")
        return pd.read_csv(input_path, dtype=str)
    else:
        print(f"Reading Excel: {input_path}")
        xl = pd.ExcelFile(input_path)
        sheet_name = sheet or xl.sheet_names[0]
        print(f"Using sheet: {sheet_name}")
        return pd.read_excel(input_path, sheet_name=sheet_name, dtype=str)


# ── Column Validation ────────────────────────────────────────────────

def _validate_columns(df: pd.DataFrame, cfg: dict[str, Any]) -> None:
    """Check required columns exist; warn about missing optional ones."""
    cols = cfg["columns"]
    vid_col = cols["hcp_vid"]
    last_col = cols["hcp_last_name"]

    missing = [c for c in [vid_col, last_col] if c not in df.columns]
    if missing:
        print("\nERROR: Missing required columns:")
        for c in missing:
            print(f"  - {c}")
        print("\nAvailable columns:")
        for c in sorted(df.columns):
            print(f"  - {c}")
        sys.exit(1)

    # Summarize what's available
    present = []
    absent = []
    check_list = [
        ("First Name", cols.get("hcp_first_name", "")),
        ("Middle Name", cols.get("hcp_middle_name", "")),
        ("Specialty 1", cols.get("specialty_1", "")),
        ("License Number", cols.get("license_number", "")),
        ("License Body", cols.get("license_body", "")),
        ("License Status", cols.get("license_status", "")),
        ("City (CDA)", cols.get("city_cda", "")),
        ("Address City", cols.get("addr_city", "")),
        ("Address State", cols.get("addr_state", "")),
        ("Address Postal", cols.get("addr_postal", "")),
        ("HCO VID", cols.get("hco_vid", "")),
        ("Parent HCO VID", cols.get("parent_hco_vid", "")),
    ]
    for label, col in check_list:
        if col and col in df.columns:
            present.append(label)
        elif col:
            absent.append(label)

    phone_pattern = cols.get("phone_pattern", "hcp.phone_*")
    email_pattern = cols.get("email_pattern", "hcp.email_*")
    phone_cols = [c for c in df.columns if c.startswith(phone_pattern.replace("*", ""))]
    email_cols = [c for c in df.columns if c.startswith(email_pattern.replace("*", ""))]

    print(f"\n{'='*60}")
    print(f"  Columns: {len(df.columns)} total")
    print(f"  Phone columns: {len(phone_cols)}")
    print(f"  Email columns: {len(email_cols)}")
    print(f"  Available signals: {', '.join(present)}")
    if absent:
        print(f"  Missing (optional): {', '.join(absent)}")
    print(f"{'='*60}\n")


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="HCP/HCO Duplicate Identification Tool v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--input", required=True, help="Input file (Excel or CSV)")
    ap.add_argument("--sheet", default=None, help="Sheet name (Excel only)")
    ap.add_argument("--config", default=None, help="Path to YAML config file")
    ap.add_argument("--outdir", default="./out", help="Output directory")
    ap.add_argument(
        "--shared-threshold", type=int, default=None,
        help="Override shared-contact threshold from config",
    )
    ap.add_argument(
        "--hcp-only", action="store_true",
        help="Skip HCO pipeline",
    )
    ap.add_argument(
        "--profile-type",
        choices=["auto", "hcp", "hco", "stockist"],
        default="auto",
        help="Entity profile type: auto (default), hcp, hco, or stockist",
    )
    ap.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging",
    )
    args = ap.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(name)-20s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # Config
    cfg = _load_config(args.config)
    if args.shared_threshold is not None:
        cfg.setdefault("shared_contact", {})["threshold"] = args.shared_threshold
    if args.profile_type != "auto":
        cfg["profile_type"] = args.profile_type

    # Load data
    t0 = time.time()
    df = _load_data(args.input, args.sheet)
    print(f"Loaded {len(df)} rows x {len(df.columns)} columns in {time.time()-t0:.1f}s")

    # Validate
    _validate_columns(df, cfg)

    # ── HCP Pipeline ─────────────────────────────────────────────────
    from .hcp_pipeline import run_hcp_pipeline

    print("\n--- HCP Duplicate Detection ---")
    t1 = time.time()
    hcp_results = run_hcp_pipeline(df, cfg)
    print(f"HCP pipeline: {time.time()-t1:.1f}s")

    hcp_sum = hcp_results.get("hcp_summary", pd.DataFrame())
    if not hcp_sum.empty:
        s = hcp_sum.iloc[0]
        print(f"  Unique VIDs:       {int(s.get('unique_hcp_vids', 0)):,}")
        print(f"  Candidate pairs:   {int(s.get('candidate_pairs_evaluated', 0)):,}")
        print(f"  AUTO-MERGE:        {int(s.get('auto_merge_pairs', 0)):,}")
        print(f"  REVIEW:            {int(s.get('review_pairs', 0)):,}")
        print(f"  NOT-DUP:           {int(s.get('not_dup_pairs', 0)):,}")
        print(f"  Clusters:          {int(s.get('auto_clusters', 0)):,}")
        # Effort Avoidance (VID-based)
        auto_vids = int(s.get("vids_auto_resolved", 0))
        review_vids = int(s.get("review_vids", 0))
        saved = s.get("effort_avoidance_hours", 0)
        remaining = s.get("est_remaining_review_hours", 0)
        velocity = s.get("manual_velocity_per_hr", 6.5)
        total_vids = int(s.get("unique_hcp_vids", 0))
        auto_pct = round(auto_vids / total_vids * 100, 1) if total_vids else 0
        print(f"\n  --- Effort Avoidance (@ {velocity} VIDs/hr) ---")
        print(f"  VIDs auto-resolved:            {auto_vids:,}")
        print(f"  Automation rate:               {auto_pct}%")
        print(f"  Est. manual hours saved:       {saved:.1f} hrs")
        print(f"  Est. remaining review effort:  {remaining:.1f} hrs")
        # Common name intelligence
        cn_promoted = int(s.get("common_name_auto_promoted", 0))
        cn_review = int(s.get("common_name_review_flagged", 0))
        if cn_promoted or cn_review:
            print(f"\n  --- Common Name Intelligence ---")
            print(f"  Uncommon name → auto-promoted:  {cn_promoted:,}")
            print(f"  Common name → manual review:    {cn_review:,}")

    # ── HCO Pipeline ─────────────────────────────────────────────────
    hco_results: dict = {
        "hco_canonical": pd.DataFrame(),
        "hco_auto": pd.DataFrame(),
        "hco_review": pd.DataFrame(),
        "hco_notdup": pd.DataFrame(),
        "hco_clusters": pd.DataFrame(),
        "hco_summary": pd.DataFrame(),
    }

    if not args.hcp_only:
        from .hco_pipeline import run_hco_pipeline

        hco_vid_col = cfg["columns"].get("hco_entity_vid", "")
        if hco_vid_col and hco_vid_col in df.columns:
            print("\n--- HCO Duplicate Detection ---")
            t2 = time.time()
            hco_results = run_hco_pipeline(df, cfg)
            print(f"HCO pipeline: {time.time()-t2:.1f}s")

            hco_sum = hco_results.get("hco_summary", pd.DataFrame())
            if not hco_sum.empty:
                s = hco_sum.iloc[0]
                print(f"  Unique HCO VIDs:   {int(s.get('unique_hco_vids', 0)):,}")
                print(f"  AUTO-MERGE:        {int(s.get('auto_merge_pairs', 0)):,}")
                print(f"  REVIEW:            {int(s.get('review_pairs', 0)):,}")
                print(f"  NOT-DUP:           {int(s.get('not_dup_pairs', 0)):,}")
                # Effort Avoidance (VID-based)
                auto_vids = int(s.get("vids_auto_resolved", 0))
                review_vids = int(s.get("review_vids", 0))
                saved = s.get("effort_avoidance_hours", 0)
                remaining = s.get("est_remaining_review_hours", 0)
                velocity = s.get("manual_velocity_per_hr", 6.5)
                total_vids = int(s.get("unique_hco_vids", 0))
                auto_pct = round(auto_vids / total_vids * 100, 1) if total_vids else 0
                print(f"\n  --- Effort Avoidance (@ {velocity} VIDs/hr) ---")
                print(f"  VIDs auto-resolved:            {auto_vids:,}")
                print(f"  Automation rate:               {auto_pct}%")
                print(f"  Est. manual hours saved:       {saved:.1f} hrs")
                print(f"  Est. remaining review effort:  {remaining:.1f} hrs")
        else:
            print("\nHCO VID column not found — skipping HCO pipeline.")
    else:
        print("\nHCO pipeline skipped (--hcp-only).")

    # ── Write Outputs ────────────────────────────────────────────────
    from .output import write_results

    print(f"\n--- Writing Outputs to {args.outdir} ---")
    files = write_results(hcp_results, hco_results, args.outdir, cfg)
    print(f"\nDone! Wrote {len(files)} files:")
    for f in files:
        print(f"  {f}")
    print(f"\nTotal time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

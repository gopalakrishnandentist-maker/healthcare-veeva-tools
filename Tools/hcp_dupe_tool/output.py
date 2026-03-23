"""
output.py — Write results to Excel (multi-sheet) and CSV files.

Produces reviewer-friendly output with enrichment columns so
that a human reviewer can act directly from the sheet without
cross-referencing VIDs.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd

logger = logging.getLogger("dupe_tool.output")


def write_results(
    hcp_results: dict[str, pd.DataFrame],
    hco_results: dict[str, pd.DataFrame],
    outdir: str,
    cfg: dict[str, Any],
) -> list[str]:
    """Write all result DataFrames to Excel and (optionally) CSV.

    Returns a list of file paths written.
    """

    os.makedirs(outdir, exist_ok=True)
    write_csv = cfg.get("output", {}).get("write_csv", True)
    written: list[str] = []

    # ── HCP Outputs ──────────────────────────────────────────────────
    hcp_main = os.path.join(outdir, "HCP_Dupe_Check.xlsx")
    hcp_canon_path = os.path.join(outdir, "HCP_1row_per_VID.xlsx")

    hcp_canonical = hcp_results.get("hcp_canonical", pd.DataFrame())
    hcp_auto = hcp_results.get("hcp_auto", pd.DataFrame())
    hcp_review = hcp_results.get("hcp_review", pd.DataFrame())
    hcp_notdup = hcp_results.get("hcp_notdup", pd.DataFrame())
    hcp_clusters = hcp_results.get("hcp_clusters", pd.DataFrame())
    hcp_shared = hcp_results.get("hcp_shared", pd.DataFrame())
    hcp_summary = hcp_results.get("hcp_summary", pd.DataFrame())

    # Canonical standalone
    if not hcp_canonical.empty:
        with pd.ExcelWriter(hcp_canon_path, engine="openpyxl") as w:
            hcp_canonical.to_excel(w, index=False, sheet_name="HCP_Canonical")
        written.append(hcp_canon_path)
        logger.info("Wrote: %s", hcp_canon_path)

    # Main workbook
    with pd.ExcelWriter(hcp_main, engine="openpyxl") as w:
        if not hcp_summary.empty:
            hcp_summary.to_excel(w, index=False, sheet_name="Summary")
        if not hcp_auto.empty:
            hcp_auto.to_excel(w, index=False, sheet_name="AUTO_MERGE")
        else:
            pd.DataFrame({"info": ["No auto-merge pairs found"]}).to_excel(
                w, index=False, sheet_name="AUTO_MERGE"
            )
        if not hcp_review.empty:
            hcp_review.to_excel(w, index=False, sheet_name="REVIEW")
        else:
            pd.DataFrame({"info": ["No review pairs found"]}).to_excel(
                w, index=False, sheet_name="REVIEW"
            )
        if not hcp_notdup.empty:
            hcp_notdup.to_excel(w, index=False, sheet_name="NOT_DUP")
        if not hcp_clusters.empty:
            hcp_clusters.to_excel(w, index=False, sheet_name="AUTO_CLUSTERS")
        if not hcp_shared.empty:
            hcp_shared.to_excel(w, index=False, sheet_name="Shared_Contacts")
    written.append(hcp_main)
    logger.info("Wrote: %s", hcp_main)

    # CSV
    if write_csv:
        csv_map = {
            "HCP_AUTO_MERGE.csv": hcp_auto,
            "HCP_REVIEW.csv": hcp_review,
            "HCP_NOT_DUP.csv": hcp_notdup,
            "HCP_AUTO_CLUSTERS.csv": hcp_clusters,
            "HCP_Shared_Contacts.csv": hcp_shared,
        }
        for fname, frame in csv_map.items():
            if not frame.empty:
                path = os.path.join(outdir, fname)
                frame.to_csv(path, index=False)
                written.append(path)

    # ── HCO Outputs ──────────────────────────────────────────────────
    hco_canonical = hco_results.get("hco_canonical", pd.DataFrame())
    hco_auto = hco_results.get("hco_auto", pd.DataFrame())
    hco_review = hco_results.get("hco_review", pd.DataFrame())
    hco_notdup = hco_results.get("hco_notdup", pd.DataFrame())
    hco_clusters = hco_results.get("hco_clusters", pd.DataFrame())
    hco_summary = hco_results.get("hco_summary", pd.DataFrame())

    if not hco_canonical.empty:
        hco_main = os.path.join(outdir, "HCO_Dupe_Check.xlsx")
        hco_canon_path = os.path.join(outdir, "HCO_1row_per_VID.xlsx")

        with pd.ExcelWriter(hco_canon_path, engine="openpyxl") as w:
            hco_canonical.to_excel(w, index=False, sheet_name="HCO_Canonical")
        written.append(hco_canon_path)

        with pd.ExcelWriter(hco_main, engine="openpyxl") as w:
            if not hco_summary.empty:
                hco_summary.to_excel(w, index=False, sheet_name="Summary")
            if not hco_auto.empty:
                hco_auto.to_excel(w, index=False, sheet_name="AUTO_MERGE")
            else:
                pd.DataFrame({"info": ["No auto-merge pairs found"]}).to_excel(
                    w, index=False, sheet_name="AUTO_MERGE"
                )
            if not hco_review.empty:
                hco_review.to_excel(w, index=False, sheet_name="REVIEW")
            else:
                pd.DataFrame({"info": ["No review pairs found"]}).to_excel(
                    w, index=False, sheet_name="REVIEW"
                )
            if not hco_notdup.empty:
                hco_notdup.to_excel(w, index=False, sheet_name="NOT_DUP")
            if not hco_clusters.empty:
                hco_clusters.to_excel(w, index=False, sheet_name="AUTO_CLUSTERS")
        written.append(hco_main)
        logger.info("Wrote: %s", hco_main)

        if write_csv:
            csv_map = {
                "HCO_AUTO_MERGE.csv": hco_auto,
                "HCO_REVIEW.csv": hco_review,
                "HCO_NOT_DUP.csv": hco_notdup,
                "HCO_AUTO_CLUSTERS.csv": hco_clusters,
            }
            for fname, frame in csv_map.items():
                if not frame.empty:
                    path = os.path.join(outdir, fname)
                    frame.to_csv(path, index=False)
                    written.append(path)
    else:
        logger.info("No HCO records — skipping HCO output files.")

    return written

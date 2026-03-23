"""
hco_pipeline.py — HCO (Healthcare Organization) duplicate detection pipeline.

Mirrors the HCP pipeline structure:
  1. Collapse exploded extract → 1 canonical row per HCO VID
  2. Build blocking keys and generate candidate pairs
  3. Apply tiered rules:  AUTO → NOT-DUP → REVIEW (scored)
  4. Cluster AUTO pairs
  5. Return results for the output writer
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import pandas as pd

from .core import (
    BlockingEngine,
    DSU,
    agg_set,
    name_similarity,
    norm_phone,
    norm_text,
)

logger = logging.getLogger("dupe_tool.hco")


# ── Canonicalization ─────────────────────────────────────────────────

def _build_hco_canonical(
    df: pd.DataFrame, cfg: dict[str, Any]
) -> pd.DataFrame:
    """Collapse multi-row-per-HCO-VID extract into one canonical record per HCO VID."""

    cols = cfg["columns"]

    vid_col = cols.get("hco_entity_vid", "")
    name_col = cols.get("hco_name", "")
    type_col = cols.get("hco_type", "")
    phone_col = cols.get("hco_phone", "")
    fax_col = cols.get("hco_fax", "")
    city_col = cols.get("hco_city", "")
    state_col = cols.get("hco_state", "")
    postal_col = cols.get("hco_postal", "")
    addr1_col = cols.get("hco_addr_line1", "")
    addr2_col = cols.get("hco_addr_line2", "")
    status_col = cols.get("hco_status", "")

    # Check required columns
    if not vid_col or vid_col not in df.columns:
        logger.warning("HCO VID column '%s' not found — skipping HCO pipeline.", vid_col)
        return pd.DataFrame()
    if not name_col or name_col not in df.columns:
        logger.warning("HCO Name column '%s' not found — skipping HCO pipeline.", name_col)
        return pd.DataFrame()

    def _safe_col(col):
        return col if col and col in df.columns else None

    records: list[dict[str, Any]] = []
    for vid, g in df.groupby(vid_col, sort=False):
        if pd.isna(vid) or not str(vid).strip():
            continue

        # Name
        raw_name = str(g[name_col].iloc[0]) if not pd.isna(g[name_col].iloc[0]) else ""
        name_norm = norm_text(raw_name)

        # Type
        raw_type = ""
        if _safe_col(type_col):
            raw_type = str(g[type_col].iloc[0]) if not pd.isna(g[type_col].iloc[0]) else ""
        type_norm = norm_text(raw_type)

        # Phone / Fax
        phones: set[str] = set()
        faxes: set[str] = set()
        if _safe_col(phone_col):
            phones = agg_set(g[phone_col], norm_phone)
        if _safe_col(fax_col):
            faxes = agg_set(g[fax_col], norm_phone)

        # Address
        city_norm = ""
        state_norm = ""
        postal_norm = ""
        addr_norm = ""
        if _safe_col(city_col):
            city_norm = norm_text(g[city_col].iloc[0])
        if _safe_col(state_col):
            state_norm = norm_text(g[state_col].iloc[0])
        if _safe_col(postal_col):
            val = g[postal_col].iloc[0]
            postal_norm = "".join(ch for ch in str(val) if ch.isdigit()) if not pd.isna(val) else ""
        if _safe_col(addr1_col):
            addr_norm = norm_text(g[addr1_col].iloc[0])
            if _safe_col(addr2_col):
                a2 = norm_text(g[addr2_col].iloc[0])
                if a2:
                    addr_norm = f"{addr_norm} {a2}".strip()

        # Status
        hco_status = ""
        if _safe_col(status_col):
            hco_status = str(g[status_col].iloc[0]) if not pd.isna(g[status_col].iloc[0]) else ""

        records.append({
            "vid": str(vid).strip(),
            "name_norm": name_norm,
            "type_norm": type_norm,
            "phone_norm": ";".join(sorted(phones)),
            "fax_norm": ";".join(sorted(faxes)),
            "phones": sorted(phones),
            "faxes": sorted(faxes),
            "city_norm": city_norm,
            "state_norm": state_norm,
            "postal_norm": postal_norm,
            "addr_norm": addr_norm,
            "hco_status": hco_status,
            # Display
            "display_name": raw_name.strip(),
            "display_type": raw_type.strip(),
            "display_city": city_norm,
            "display_state": state_norm,
            "display_phones": "; ".join(sorted(phones)[:3]),
        })

    canon = pd.DataFrame(records)
    logger.info("Canonicalized %d unique HCO VIDs.", len(canon))
    return canon


# ── Signal Computation ───────────────────────────────────────────────

def _compute_hco_signals(ar: dict, br: dict) -> dict[str, Any]:
    """Compute all match signals between two canonical HCO records."""

    nscore = name_similarity(ar["name_norm"], br["name_norm"])

    phone_ov = set(ar["phones"]) & set(br["phones"])
    fax_ov = set(ar["faxes"]) & set(br["faxes"])

    type_match = ar["type_norm"] and ar["type_norm"] == br["type_norm"]
    type_diff = ar["type_norm"] and br["type_norm"] and ar["type_norm"] != br["type_norm"]

    city_match = ar["city_norm"] and ar["city_norm"] == br["city_norm"]
    city_diff = ar["city_norm"] and br["city_norm"] and ar["city_norm"] != br["city_norm"]
    state_match = ar["state_norm"] and ar["state_norm"] == br["state_norm"]
    postal_match = ar["postal_norm"] and ar["postal_norm"] == br["postal_norm"]

    # Address overlap (simplified: check if normalized addr lines share significant tokens)
    addr_overlap = False
    if ar["addr_norm"] and br["addr_norm"]:
        a_tokens = set(ar["addr_norm"].split())
        b_tokens = set(br["addr_norm"].split())
        # Remove very common tokens
        common_stop = {"st", "street", "road", "rd", "ave", "avenue", "dr", "drive",
                       "lane", "ln", "blvd", "boulevard", "floor", "fl", "suite", "ste",
                       "unit", "building", "bldg", "no", "number"}
        a_tokens -= common_stop
        b_tokens -= common_stop
        if a_tokens and b_tokens:
            overlap_ratio = len(a_tokens & b_tokens) / min(len(a_tokens), len(b_tokens))
            addr_overlap = overlap_ratio >= 0.5

    address_match = (postal_match and city_match) or (addr_overlap and city_match)

    return {
        "nscore": nscore,
        "phone_overlap": phone_ov,
        "fax_overlap": fax_ov,
        "phone_or_fax_overlap": bool(phone_ov) or bool(fax_ov),
        "type_match": type_match,
        "type_different": type_diff,
        "city_match": city_match,
        "city_different": city_diff,
        "state_match": state_match,
        "postal_match": postal_match,
        "address_overlap": address_match,
    }


# ── Rule Application ─────────────────────────────────────────────────

def _apply_hco_auto_rules(sig: dict, cfg: dict) -> tuple[str, str] | None:
    """Check HCO AUTO-MERGE rules."""

    ns = sig["nscore"]
    rules = cfg.get("hco_auto_rules", {})

    # H1: Name + Address + Phone/Fax
    r = rules.get("H1_name_addr_phone", {})
    if ns >= r.get("name_min", 90) and sig["address_overlap"] and sig["phone_or_fax_overlap"]:
        return "H1_NAME_ADDR_PHONE", f"Name + Address + Phone/Fax match (name sim: {ns:.0f}%)"

    # H2: Name + Address + Same type
    r = rules.get("H2_name_addr_type", {})
    if ns >= r.get("name_min", 92) and sig["address_overlap"] and sig["type_match"]:
        return "H2_NAME_ADDR_TYPE", f"Name + Address + Same type (name sim: {ns:.0f}%)"

    # H3: Name + Phone/Fax + Same type
    r = rules.get("H3_name_phone_type", {})
    if ns >= r.get("name_min", 90) and sig["phone_or_fax_overlap"] and sig["type_match"]:
        return "H3_NAME_PHONE_TYPE", f"Name + Phone/Fax + Same type (name sim: {ns:.0f}%)"

    return None


def _score_hco_review(sig: dict, ar: dict, br: dict, cfg: dict) -> tuple[int, list[str], str]:
    """Compute a review score for HCO pairs."""

    w = cfg.get("hco_review_scoring", {})
    score = 0
    reasons: list[str] = []
    ns = sig["nscore"]

    if ns >= 92:
        score += w.get("name_strong", 35)
        reasons.append("NAME_STRONG")
    elif ns >= 85:
        score += w.get("name_medium", 25)
        reasons.append("NAME_MED")
    elif ns >= 75:
        score += w.get("name_weak", 15)
        reasons.append("NAME_WEAK")

    if sig["address_overlap"]:
        score += w.get("address_match", 30)
        reasons.append("ADDR_MATCH")
    if sig["city_match"]:
        score += w.get("city_match", 20)
        reasons.append("CITY_MATCH")
    if sig["postal_match"]:
        score += w.get("postal_match", 25)
        reasons.append("POSTAL_MATCH")
    if sig["phone_overlap"]:
        score += w.get("phone_match", 20)
        reasons.append("PHONE_MATCH")
    if sig["fax_overlap"]:
        score += w.get("fax_match", 15)
        reasons.append("FAX_MATCH")
    if sig["type_match"]:
        score += w.get("type_match", 15)
        reasons.append("TYPE_MATCH")
    if sig["state_match"]:
        score += w.get("state_match", 10)
        reasons.append("STATE_MATCH")

    # Negatives
    if sig["type_different"]:
        score += w.get("different_type", -20)
        reasons.append("DIFF_TYPE")
    if sig["city_different"]:
        score += w.get("different_city", -15)
        reasons.append("DIFF_CITY")

    high = w.get("high_confidence", 80)
    med = w.get("medium", 60)
    if score >= high:
        comment = f"High confidence (Score: {score})"
    elif score >= med:
        comment = f"Medium confidence (Score: {score})"
    else:
        comment = f"Low confidence (Score: {score})"

    return score, reasons, comment


# ── Enrichment ───────────────────────────────────────────────────────

def _enrich_hco_row(vid_a: str, vid_b: str, canon_idx: pd.DataFrame) -> dict:
    ar = canon_idx.loc[vid_a]
    br = canon_idx.loc[vid_b]
    return {
        "name_a": ar["display_name"],
        "name_b": br["display_name"],
        "type_a": ar["display_type"],
        "type_b": br["display_type"],
        "city_a": ar["display_city"],
        "city_b": br["display_city"],
        "phones_a": ar["display_phones"],
        "phones_b": br["display_phones"],
    }


# ── Main Pipeline ────────────────────────────────────────────────────

def run_hco_pipeline(
    df: pd.DataFrame, cfg: dict[str, Any]
) -> dict[str, pd.DataFrame]:
    """Execute the full HCO dedup pipeline and return result DataFrames."""

    enrich = cfg.get("output", {}).get("enrich_output", True)
    review_threshold = cfg.get("hco_review_scoring", {}).get("review_threshold", 50)

    # Step 1 — Canonicalize
    canon = _build_hco_canonical(df, cfg)
    if canon.empty:
        logger.warning("No HCO records found — skipping HCO pipeline.")
        return {
            "hco_canonical": pd.DataFrame(),
            "hco_auto": pd.DataFrame(),
            "hco_review": pd.DataFrame(),
            "hco_notdup": pd.DataFrame(),
            "hco_clusters": pd.DataFrame(),
            "hco_summary": pd.DataFrame(),
        }

    canon_idx = canon.set_index("vid", drop=False)

    # Step 2 — Blocking
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=False,  # Not applicable for HCOs
    )
    for _, row in canon.iterrows():
        blocker.add_hco(row["vid"], row.to_dict())
    pairs = blocker.candidate_pairs()

    # Step 3 — Classify pairs
    auto_rows: list[dict] = []
    review_rows: list[dict] = []
    notdup_rows: list[dict] = []

    for a, b in sorted(pairs):
        ar = canon_idx.loc[a].to_dict()
        br = canon_idx.loc[b].to_dict()
        sig = _compute_hco_signals(ar, br)

        base = {
            "vid_a": a,
            "vid_b": b,
            "name_similarity": round(sig["nscore"], 1),
            "type_match": int(sig["type_match"]),
            "city_match": int(sig["city_match"]),
            "postal_match": int(sig["postal_match"]),
            "address_match": int(sig["address_overlap"]),
            "phone_fax_match": int(sig["phone_or_fax_overlap"]),
        }
        if enrich:
            base.update(_enrich_hco_row(a, b, canon_idx))

        # AUTO
        auto_result = _apply_hco_auto_rules(sig, cfg)
        if auto_result:
            rule, comment = auto_result
            base["rule"] = rule
            base["comments"] = comment
            auto_rows.append(base)
            continue

        # Score for REVIEW
        score, reasons, comment = _score_hco_review(sig, ar, br, cfg)
        if score >= review_threshold:
            base["score"] = score
            base["reasons"] = ",".join(reasons)
            base["comments"] = comment
            review_rows.append(base)
        else:
            base["reason"] = "HN1_LOW_SCORE"
            base["comments"] = f"Insufficient similarity (Score: {score})"
            notdup_rows.append(base)

    # Cluster
    dsu = DSU()
    for row in auto_rows:
        dsu.union(row["vid_a"], row["vid_b"])
    clusters: dict[str, set] = defaultdict(set)
    for row in auto_rows:
        root = dsu.find(row["vid_a"])
        clusters[root].add(row["vid_a"])
        clusters[root].add(row["vid_b"])
    cluster_rows = []
    for cid, (_, members) in enumerate(sorted(clusters.items()), start=1):
        for vid in sorted(members):
            enr = {}
            if enrich and vid in canon_idx.index:
                r = canon_idx.loc[vid]
                enr = {"name": r["display_name"], "type": r["display_type"], "city": r["display_city"]}
            cluster_rows.append({"cluster_id": cid, "vid": vid, **enr})

    auto_df = pd.DataFrame(auto_rows) if auto_rows else pd.DataFrame()
    review_df = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()
    notdup_df = pd.DataFrame(notdup_rows) if notdup_rows else pd.DataFrame()
    cluster_df = pd.DataFrame(cluster_rows) if cluster_rows else pd.DataFrame()

    if not review_df.empty and "score" in review_df.columns:
        review_df = review_df.sort_values("score", ascending=False).reset_index(drop=True)

    summary = {
        "unique_hco_vids": len(canon),
        "candidate_pairs_evaluated": len(pairs),
        "auto_merge_pairs": len(auto_df),
        "review_pairs": len(review_df),
        "not_dup_pairs": len(notdup_df),
        "auto_clusters": cluster_df["cluster_id"].nunique() if not cluster_df.empty else 0,
    }

    logger.info(
        "HCO Pipeline complete: %d AUTO, %d REVIEW, %d NOT_DUP, %d clusters",
        summary["auto_merge_pairs"],
        summary["review_pairs"],
        summary["not_dup_pairs"],
        summary["auto_clusters"],
    )

    return {
        "hco_canonical": canon,
        "hco_auto": auto_df,
        "hco_review": review_df,
        "hco_notdup": notdup_df,
        "hco_clusters": cluster_df,
        "hco_summary": pd.DataFrame([summary]),
    }

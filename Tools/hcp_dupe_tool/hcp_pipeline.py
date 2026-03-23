"""
hcp_pipeline.py — HCP-specific duplicate detection pipeline.

Responsibilities:
  1. Collapse exploded extract → 1 canonical row per VID
  2. Build blocking keys and generate candidate pairs
  3. Apply tiered rules:  AUTO → NOT-DUP → REVIEW (scored)
  4. Cluster AUTO pairs via union-find
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
    SharedContactDetector,
    agg_set,
    name_similarity,
    norm_email,
    norm_license,
    norm_phone,
    norm_text,
    strip_name_suffixes,
)

logger = logging.getLogger("dupe_tool.hcp")


# ── Canonicalization ─────────────────────────────────────────────────

def _build_canonical(
    df: pd.DataFrame, cfg: dict[str, Any]
) -> pd.DataFrame:
    """Collapse multi-row-per-VID extract into one canonical record per VID."""

    cols = cfg["columns"]
    suffixes = cfg.get("name_matching", {}).get("strip_suffixes", [])

    vid_col = cols["hcp_vid"]
    last_col = cols["hcp_last_name"]
    first_col = cols.get("hcp_first_name", "")
    mid_col = cols.get("hcp_middle_name", "")
    full_col = cols.get("hcp_full_name", "")

    spec_cols = [
        cols.get(f"specialty_{i}", "")
        for i in range(1, 5)
    ]
    spec_cols = [c for c in spec_cols if c and c in df.columns]

    lic_num_col = cols.get("license_number", "")
    lic_body_col = cols.get("license_body", "")
    lic_stat_col = cols.get("license_status", "")
    has_license = all(
        c and c in df.columns for c in [lic_num_col, lic_body_col, lic_stat_col]
    )

    hco_cols_cfg = [
        cols.get("hco_vid", ""),
        cols.get("parent_hco_vid", ""),
        cols.get("grandparent_hco_vid", ""),
    ]
    hco_cols = [c for c in hco_cols_cfg if c and c in df.columns]

    city_cda_col = cols.get("city_cda", "")
    addr_city_col = cols.get("addr_city", "")
    addr_state_col = cols.get("addr_state", "")
    addr_postal_col = cols.get("addr_postal", "")

    status_col = cols.get("hcp_status", "")
    cand_col = cols.get("candidate_record", "")

    phone_pattern = cols.get("phone_pattern", "hcp.phone_*")
    email_pattern = cols.get("email_pattern", "hcp.email_*")
    phone_cols = [c for c in df.columns if _glob_match(c, phone_pattern)]
    email_cols = [c for c in df.columns if _glob_match(c, email_pattern)]

    logger.info(
        "Detected %d phone cols, %d email cols, %d specialty cols, "
        "%d HCO cols, license=%s",
        len(phone_cols), len(email_cols), len(spec_cols),
        len(hco_cols), has_license,
    )

    # Pre-compute normalized columns
    def _safe_norm(col, fn=norm_text):
        return df[col].apply(fn) if col and col in df.columns else pd.Series([""] * len(df), index=df.index)

    df = df.copy()
    df["_last_norm"] = _safe_norm(last_col)
    df["_first_norm"] = _safe_norm(first_col) if first_col and first_col in df.columns else ""
    df["_mid_norm"] = _safe_norm(mid_col) if mid_col and mid_col in df.columns else ""
    df["_city_cda_norm"] = _safe_norm(city_cda_col)
    df["_addr_city_norm"] = _safe_norm(addr_city_col)
    df["_addr_state_norm"] = _safe_norm(addr_state_col)
    df["_pin_norm"] = (
        df[addr_postal_col].apply(
            lambda x: _digits_only(x) if not pd.isna(x) else ""
        )
        if addr_postal_col and addr_postal_col in df.columns
        else pd.Series([""] * len(df), index=df.index)
    )

    records: list[dict[str, Any]] = []
    for vid, g in df.groupby(vid_col, sort=False):

        # ── Name ────────────────────────────────
        parts = []
        for nc in ["_first_norm", "_mid_norm", "_last_norm"]:
            vals = g[nc] if nc in g.columns else pd.Series([""])
            v = vals.iloc[0]
            if isinstance(v, str) and v:
                parts.append(v)
        name_canon = " ".join(parts).strip()
        if not name_canon and full_col and full_col in df.columns:
            name_canon = norm_text(g[full_col].iloc[0])
        if suffixes:
            name_canon = strip_name_suffixes(name_canon, suffixes)

        first_name_norm = norm_text(g[first_col].iloc[0]) if first_col and first_col in df.columns else ""
        last_name_norm = norm_text(g[last_col].iloc[0])

        # ── Contacts ────────────────────────────
        phones: set[str] = set()
        for col in phone_cols:
            phones |= agg_set(g[col], norm_phone)
        emails: set[str] = set()
        for col in email_cols:
            emails |= agg_set(g[col], norm_email)

        # ── Licenses ────────────────────────────
        lic_tuples: set[tuple[str, str]] = set()
        active_lic_tuples: set[tuple[str, str]] = set()
        if has_license:
            for _, r in g[[lic_num_col, lic_body_col, lic_stat_col]].dropna(how="all").iterrows():
                ln = norm_license(r[lic_num_col])
                lb = norm_text(r[lic_body_col])
                if not ln and not lb:
                    continue
                t = (ln or "", lb or "")
                lic_tuples.add(t)
                status = str(r.get(lic_stat_col, "")).strip().upper()
                if status in {"A", "ACTIVE"}:
                    active_lic_tuples.add(t)

        # ── HCO VIDs ───────────────────────────
        hco_vids: set[str] = set()
        for hc in hco_cols:
            for val in g[hc]:
                nv = norm_text(val)
                if nv:
                    hco_vids.add(nv)

        # ── Geo ─────────────────────────────────
        pins = set(g["_pin_norm"].astype(str).str.strip()) - {"", "0"}
        cities = set(g["_city_cda_norm"].astype(str).str.strip())
        cities |= set(g["_addr_city_norm"].astype(str).str.strip())
        cities.discard("")
        states = set(g["_addr_state_norm"].astype(str).str.strip())
        states.discard("")
        city_cda_val = norm_text(g[city_cda_col].iloc[0]) if city_cda_col and city_cda_col in df.columns else ""

        # ── Specialties ─────────────────────────
        specialties: set[str] = set()
        for sc in spec_cols:
            for val in g[sc]:
                nv = norm_text(val)
                if nv:
                    specialties.add(nv)

        # ── Status ──────────────────────────────
        hcp_status = ""
        if status_col and status_col in g.columns:
            hcp_status = str(g[status_col].iloc[0]) if not pd.isna(g[status_col].iloc[0]) else ""
        candidate = ""
        if cand_col and cand_col in g.columns:
            candidate = str(g[cand_col].iloc[0]) if not pd.isna(g[cand_col].iloc[0]) else ""

        # ── Raw display values for enrichment ───
        raw_first = str(g[first_col].iloc[0]) if first_col and first_col in df.columns and not pd.isna(g[first_col].iloc[0]) else ""
        raw_last = str(g[last_col].iloc[0]) if not pd.isna(g[last_col].iloc[0]) else ""
        raw_specs = []
        for sc in spec_cols:
            val = g[sc].iloc[0]
            if not pd.isna(val):
                raw_specs.append(str(val).strip())
        raw_cities_list = sorted(cities)[:3]

        records.append({
            "vid": str(vid),
            "name_canon": name_canon,
            "first_name_norm": first_name_norm,
            "last_name_norm": last_name_norm,
            "city_cda_norm": city_cda_val,
            "specialties": sorted(specialties),
            "phones": sorted(phones),
            "emails": sorted(emails),
            "licenses": sorted(lic_tuples),
            "active_licenses": sorted(active_lic_tuples),
            "hco_vids": sorted(hco_vids),
            "pins": sorted(pins),
            "cities": sorted(cities),
            "states": sorted(states),
            "hcp_status": hcp_status,
            "candidate_record": candidate,
            # Enrichment display values
            "display_name": f"{raw_first} {raw_last}".strip(),
            "display_specialties": "; ".join(raw_specs),
            "display_cities": "; ".join(raw_cities_list),
            "display_phones": "; ".join(sorted(phones)[:3]),
            "display_emails": "; ".join(sorted(emails)[:3]),
        })

    canon = pd.DataFrame(records)
    logger.info("Canonicalized %d unique HCP VIDs from %d raw rows.", len(canon), len(df))
    return canon


# ── Signal Computation ───────────────────────────────────────────────

def _compute_signals(
    ar: dict, br: dict, shared_detector: SharedContactDetector | None = None
) -> dict[str, Any]:
    """Compute all match signals between two canonical HCP records.

    Args:
        ar: Record A as dict (works with both Series and plain dicts)
        br: Record B as dict
        shared_detector: SharedContactDetector instance (required for accurate shared contact detection)

    Returns:
        Dictionary of signal computations
    """

    phones_ov = set(ar["phones"]) & set(br["phones"])
    emails_ov = set(ar["emails"]) & set(br["emails"])
    spec_ov = set(ar["specialties"]) & set(br["specialties"])
    cities_ov = set(ar["cities"]) & set(br["cities"])
    pins_ov = set(ar["pins"]) & set(br["pins"])
    states_ov = set(ar["states"]) & set(br["states"])
    hco_ov = set(ar["hco_vids"]) & set(br["hco_vids"])

    nscore = name_similarity(ar["name_canon"], br["name_canon"])

    # License overlap
    a_lic = set(ar["licenses"])
    b_lic = set(br["licenses"])
    lic_match = bool(a_lic & b_lic)
    a_act = set(ar["active_licenses"])
    b_act = set(br["active_licenses"])
    active_lic_match = bool(a_act & b_act)
    active_lic_conflict = bool(a_act) and bool(b_act) and not bool(a_act & b_act)

    # Shared contact check
    shared = shared_detector.is_shared(phones_ov, emails_ov) if shared_detector else False

    # Geo support
    geo = bool(pins_ov) or bool(cities_ov) or bool(states_ov)
    if ar["city_cda_norm"] and ar["city_cda_norm"] == br["city_cda_norm"]:
        geo = True

    # City explicitly different
    a_has_cities = bool(ar["cities"])
    b_has_cities = bool(br["cities"])
    cities_diff = a_has_cities and b_has_cities and not cities_ov

    # License info string
    lic_info = _license_info_str(a_lic, b_lic)

    return {
        "nscore": nscore,
        "phones_overlap": phones_ov,
        "emails_overlap": emails_ov,
        "spec_overlap": spec_ov,
        "cities_overlap": cities_ov,
        "pins_overlap": pins_ov,
        "hco_overlap": hco_ov,
        "geo_support": geo,
        "cities_explicitly_different": cities_diff,
        "license_match": lic_match,
        "active_license_match": active_lic_match,
        "active_license_conflict": active_lic_conflict,
        "shared_contact": shared,
        "license_info": lic_info,
    }


def _license_info_str(a_lic: set, b_lic: set) -> str:
    if not a_lic and not b_lic:
        return "NO_LICENSE"
    overlap = a_lic & b_lic
    if overlap:
        nums = [ln for ln, _ in overlap if ln]
        return f"SAME_{nums[0]}" if nums else "SAME_UNKNOWN"
    a_nums = [ln for ln, _ in a_lic if ln]
    b_nums = [ln for ln, _ in b_lic if ln]
    if a_nums and b_nums:
        return f"DIFF_{a_nums[0]}_{b_nums[0]}"
    if a_nums:
        return f"ONLY_A_{a_nums[0]}"
    if b_nums:
        return f"ONLY_B_{b_nums[0]}"
    return "NO_LICENSE"


# ── Rule Application ─────────────────────────────────────────────────

def _apply_auto_rules(sig: dict, cfg: dict) -> tuple[str, str] | None:
    """Check AUTO-MERGE rules in priority order. Returns (rule_code, comment) or None."""

    ns = sig["nscore"]
    spec_str = ",".join(sorted(sig["spec_overlap"]))

    rules = cfg.get("hcp_auto_rules", {})

    # G1: Name + Specialty + HCO
    r = rules.get("G1_name_spec_hco", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["spec_overlap"]
        and sig["hco_overlap"]
    ):
        cmt = f"Name + Specialty + HCO match (Spec: {spec_str})"
        if sig["active_license_match"]:
            cmt += " [+License match]"
        return "G1_NAME_SPL_HCO", cmt

    # G2: Name + Specialty + PIN
    r = rules.get("G2_name_spec_pin", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["spec_overlap"]
        and sig["pins_overlap"]
    ):
        pins_str = ",".join(sorted(sig["pins_overlap"])[:3])
        cmt = f"Name + Specialty + Postal match (Spec: {spec_str}, PIN: {pins_str})"
        if sig["active_license_match"]:
            cmt += " [+License match]"
        return "G2_NAME_SPL_PIN", cmt

    # G3: Name + Specialty + City
    r = rules.get("G3_name_spec_city", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["spec_overlap"]
        and sig["cities_overlap"]
    ):
        cities_str = ",".join(sorted(sig["cities_overlap"])[:3])
        cmt = f"Name + Specialty + City match (Spec: {spec_str}, City: {cities_str})"
        if sig["active_license_match"]:
            cmt += " [+License match]"
        return "G3_NAME_SPL_CITY", cmt

    # G4: Active license match + reasonable name
    r = rules.get("G4_license_match", {})
    if (
        ns >= r.get("name_min", 80)
        and sig["active_license_match"]
    ):
        cmt = f"Active license match + name similarity {ns:.0f}%"
        if sig["spec_overlap"]:
            cmt += f" [+Spec: {spec_str}]"
        return "G4_LICENSE", cmt

    # G5: Phone + Email (non-shared)
    r = rules.get("G5_phone_email", {})
    if (
        ns >= r.get("name_min", 85)
        and sig["phones_overlap"]
        and sig["emails_overlap"]
        and not sig["shared_contact"]
    ):
        ph_str = ";".join(sorted(sig["phones_overlap"]))[:80]
        em_str = ";".join(sorted(sig["emails_overlap"]))[:80]
        cmt = f"Phone + Email match (Ph: {ph_str}, Em: {em_str})"
        return "G5_PHONE_EMAIL", cmt

    # G6: Email + strong name (non-shared)
    r = rules.get("G6_email_name", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["emails_overlap"]
        and not sig["shared_contact"]
    ):
        em_str = ";".join(sorted(sig["emails_overlap"]))[:80]
        cmt = f"Email + Strong name match (Em: {em_str})"
        return "G6_EMAIL_NAME", cmt

    return None


def _apply_not_dup_rules(sig: dict, ar: dict, br: dict, cfg: dict) -> tuple[str, str] | None:
    """Check NOT-DUPLICATE rules. Returns (rule_code, comment) or None."""

    ns = sig["nscore"]
    rules = cfg.get("hcp_not_dup_rules", {})

    # N1: Active license conflict
    r = rules.get("N1_active_license_conflict", {})
    if sig["active_license_conflict"]:
        # Exception: strong contact overlap
        if sig["phones_overlap"] and sig["emails_overlap"]:
            pass  # fall through
        else:
            return "N1_LICENSE_CONFLICT", (
                "Both have active licenses but none match — different practitioners"
            )

    # N2: Same name + specialty, explicitly different cities
    r = rules.get("N2_different_cities", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["spec_overlap"]
        and sig["cities_explicitly_different"]
    ):
        # Exception: strong contact or license match
        if (sig["phones_overlap"] and sig["emails_overlap"]) or sig["license_match"]:
            pass  # fall through
        else:
            a_city = ",".join(sorted(ar["cities"])[:2])
            b_city = ",".join(sorted(br["cities"])[:2])
            return "N2_DIFFERENT_CITIES", (
                f"Same name + specialty but different cities (A: {a_city}, B: {b_city})"
            )

    return None


def _score_review(sig: dict, ar: dict, br: dict, cfg: dict) -> tuple[int, list[str], str]:
    """Compute a review score and generate a human-readable comment."""

    w = cfg.get("hcp_review_scoring", {})
    score = 0
    reasons: list[str] = []
    ns = sig["nscore"]

    # ── Positive signals ────────────────────
    if ns >= cfg.get("name_matching", {}).get("strong", 92):
        score += w.get("name_strong", 35)
        reasons.append("NAME_STRONG")
    elif ns >= cfg.get("name_matching", {}).get("medium", 85):
        score += w.get("name_medium", 25)
        reasons.append("NAME_MED")
    elif ns >= cfg.get("name_matching", {}).get("weak", 75):
        score += w.get("name_weak", 15)
        reasons.append("NAME_WEAK")

    if sig["spec_overlap"]:
        score += w.get("specialty_match", 30)
        reasons.append(f"SPEC_MATCH({','.join(sorted(sig['spec_overlap']))})")
    elif ar["specialties"] and br["specialties"]:
        score += w.get("specialty_conflict", -15)
        reasons.append("SPEC_CONFLICT")

    if sig["hco_overlap"]:
        score += w.get("hco_overlap", 25)
        reasons.append("HCO_MATCH")
    if sig["pins_overlap"]:
        score += w.get("pin_match", 25)
        reasons.append("PIN_MATCH")
    elif sig["cities_overlap"]:
        score += w.get("city_match", 20)
        reasons.append("CITY_MATCH")
    elif sig["geo_support"]:
        score += w.get("geo_support", 10)
        reasons.append("GEO_SUPPORT")

    if sig["license_match"]:
        score += w.get("license_match", 40)
        reasons.append("LICENSE_MATCH")

    if sig["phones_overlap"] and sig["emails_overlap"] and not sig["shared_contact"]:
        score += w.get("phone_email_match", 20)
        reasons.append("PHONE_EMAIL")
    elif sig["emails_overlap"] and not sig["shared_contact"]:
        score += w.get("email_match", 15)
        reasons.append("EMAIL")
    elif sig["phones_overlap"] and not sig["shared_contact"]:
        score += w.get("phone_match", 10)
        reasons.append("PHONE")

    # ── Negative signals ────────────────────
    if sig["cities_explicitly_different"]:
        score += w.get("different_cities", -20)
        reasons.append("DIFF_CITIES")
    if str(ar.get("candidate_record", "")).strip().lower() == "true":
        score += w.get("candidate_record", -10)
        reasons.append("CAND_A")
    if str(br.get("candidate_record", "")).strip().lower() == "true":
        score += w.get("candidate_record", -10)
        reasons.append("CAND_B")

    # ── Comment ─────────────────────────────
    high = w.get("high_confidence", 80)
    med_high = w.get("medium_high", 70)
    med = w.get("medium", 60)

    if score >= high:
        comment = f"High confidence (Score: {score})"
    elif score >= med_high:
        comment = f"Medium-high confidence (Score: {score})"
    elif score >= med:
        comment = f"Medium confidence (Score: {score})"
    else:
        comment = f"Low confidence (Score: {score})"

    if sig["spec_overlap"]:
        comment += f" — Specialty: {','.join(sorted(sig['spec_overlap']))}"
    if sig["license_match"]:
        comment += f" [License match: {sig['license_info']}]"
    if sig["active_license_conflict"]:
        comment += " [WARNING: Active license conflict]"

    return score, reasons, comment


# ── Enrichment helpers ───────────────────────────────────────────────

def _enrich_row(vid_a: str, vid_b: str, canon_idx: pd.DataFrame | dict) -> dict:
    """Pull display columns for both VIDs to add to output row.

    Args:
        vid_a: First VID
        vid_b: Second VID
        canon_idx: Either a pandas DataFrame indexed by 'vid', or a plain dict {vid: record_dict}

    Returns:
        Enrichment dict with display columns
    """
    if isinstance(canon_idx, dict):
        ar = canon_idx.get(vid_a, {})
        br = canon_idx.get(vid_b, {})
    else:
        ar = canon_idx.loc[vid_a]
        br = canon_idx.loc[vid_b]

    return {
        "name_a": ar.get("display_name", "") if isinstance(ar, dict) else ar["display_name"],
        "name_b": br.get("display_name", "") if isinstance(br, dict) else br["display_name"],
        "specialties_a": ar.get("display_specialties", "") if isinstance(ar, dict) else ar["display_specialties"],
        "specialties_b": br.get("display_specialties", "") if isinstance(br, dict) else br["display_specialties"],
        "cities_a": ar.get("display_cities", "") if isinstance(ar, dict) else ar["display_cities"],
        "cities_b": br.get("display_cities", "") if isinstance(br, dict) else br["display_cities"],
        "phones_a": ar.get("display_phones", "") if isinstance(ar, dict) else ar["display_phones"],
        "phones_b": br.get("display_phones", "") if isinstance(br, dict) else br["display_phones"],
        "emails_a": ar.get("display_emails", "") if isinstance(ar, dict) else ar["display_emails"],
        "emails_b": br.get("display_emails", "") if isinstance(br, dict) else br["display_emails"],
    }


# ── Main Pipeline ────────────────────────────────────────────────────

def run_hcp_pipeline(
    df: pd.DataFrame, cfg: dict[str, Any]
) -> dict[str, pd.DataFrame]:
    """Execute the full HCP dedup pipeline and return result DataFrames."""

    enrich = cfg.get("output", {}).get("enrich_output", True)
    max_contact = cfg.get("output", {}).get("max_contact_display", 200)
    review_threshold = cfg.get("hcp_review_scoring", {}).get("review_threshold", 50)

    # Step 1 — Canonicalize
    canon = _build_canonical(df, cfg)
    canon_idx = canon.set_index("vid", drop=False)

    # Step 2 — Shared contacts
    shared_det = SharedContactDetector(
        threshold=cfg.get("shared_contact", {}).get("threshold", 5)
    )
    for _, row in canon.iterrows():
        shared_det.feed(set(row["phones"]), set(row["emails"]))
    shared_det.finalize()

    # Step 3 — Blocking
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=blk_cfg.get("first_initial_blocking", True),
    )
    for _, row in canon.iterrows():
        blocker.add_hcp(row["vid"], row.to_dict())
    pairs = blocker.candidate_pairs()

    # Step 4 — Classify pairs
    auto_rows: list[dict] = []
    review_rows: list[dict] = []
    notdup_rows: list[dict] = []

    for a, b in sorted(pairs):
        ar = canon_idx.loc[a].to_dict()
        br = canon_idx.loc[b].to_dict()
        sig = _compute_signals(ar, br, shared_det)

        base = {
            "vid_a": a,
            "vid_b": b,
            "name_similarity": round(sig["nscore"], 1),
            "specialty_match": ",".join(sorted(sig["spec_overlap"])),
            "geo_support": int(sig["geo_support"]),
            "hco_overlap": int(bool(sig["hco_overlap"])),
            "matched_phones": ";".join(sorted(sig["phones_overlap"]))[:max_contact],
            "matched_emails": ";".join(sorted(sig["emails_overlap"]))[:max_contact],
            "license_info": sig["license_info"],
        }
        if enrich:
            base.update(_enrich_row(a, b, canon_idx))

        # Try AUTO
        auto_result = _apply_auto_rules(sig, cfg)
        if auto_result:
            rule, comment = auto_result
            base["rule"] = rule
            base["comments"] = comment
            auto_rows.append(base)
            continue

        # Try NOT-DUP
        notdup_result = _apply_not_dup_rules(sig, ar, br, cfg)
        if notdup_result:
            reason, comment = notdup_result
            base["reason"] = reason
            base["comments"] = comment
            notdup_rows.append(base)
            continue

        # Score for REVIEW
        score, reasons, comment = _score_review(sig, ar, br, cfg)
        if score >= review_threshold:
            base["score"] = score
            base["reasons"] = ",".join(reasons)
            base["comments"] = comment
            review_rows.append(base)
        else:
            base["reason"] = "N3_LOW_SCORE"
            base["comments"] = f"Insufficient similarity (Score: {score})"
            notdup_rows.append(base)

    # Step 5 — Cluster AUTO pairs
    dsu = DSU()
    for row in auto_rows:
        dsu.union(row["vid_a"], row["vid_b"])
    clusters: dict[str, set[str]] = defaultdict(set)
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
                enr = {
                    "name": r["display_name"],
                    "specialties": r["display_specialties"],
                    "cities": r["display_cities"],
                }
            cluster_rows.append({"cluster_id": cid, "vid": vid, **enr})

    # Build DataFrames
    auto_df = pd.DataFrame(auto_rows) if auto_rows else pd.DataFrame()
    review_df = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()
    notdup_df = pd.DataFrame(notdup_rows) if notdup_rows else pd.DataFrame()
    cluster_df = pd.DataFrame(cluster_rows) if cluster_rows else pd.DataFrame()
    shared_rows = shared_det.shared_rows()
    shared_df = pd.DataFrame(shared_rows, columns=["type", "value", "vid_count"]) if shared_rows else pd.DataFrame()

    # Sort review by score descending
    if not review_df.empty and "score" in review_df.columns:
        review_df = review_df.sort_values("score", ascending=False).reset_index(drop=True)

    # Summary
    summary = {
        "raw_rows": len(df),
        "unique_hcp_vids": len(canon),
        "candidate_pairs_evaluated": len(pairs),
        "auto_merge_pairs": len(auto_df),
        "review_pairs": len(review_df),
        "not_dup_pairs": len(notdup_df),
        "auto_clusters": cluster_df["cluster_id"].nunique() if not cluster_df.empty else 0,
        "shared_phones": len(shared_det._shared_phones),
        "shared_emails": len(shared_det._shared_emails),
    }

    logger.info(
        "HCP Pipeline complete: %d AUTO, %d REVIEW, %d NOT_DUP, %d clusters",
        summary["auto_merge_pairs"],
        summary["review_pairs"],
        summary["not_dup_pairs"],
        summary["auto_clusters"],
    )

    return {
        "hcp_canonical": canon,
        "hcp_auto": auto_df,
        "hcp_review": review_df,
        "hcp_notdup": notdup_df,
        "hcp_clusters": cluster_df,
        "hcp_shared": shared_df,
        "hcp_summary": pd.DataFrame([summary]),
    }


# ── Helpers ──────────────────────────────────────────────────────────

def _glob_match(col_name: str, pattern: str) -> bool:
    """Simple glob: 'hcp.phone_*' matches 'hcp.phone_1__v (PHONE 1)'."""
    if "*" in pattern:
        prefix = pattern.replace("*", "")
        return col_name.startswith(prefix)
    return col_name == pattern


def _digits_only(x) -> str:
    return "".join(ch for ch in str(x) if ch.isdigit())

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

try:
    from .core import (
        BlockingEngine,
        DSU,
        agg_set,
        name_similarity,
        norm_phone,
        norm_text,
        strip_hco_name_affixes,
        resolve_hco_column,
        is_acronym_of,
        compute_acronym,
    )
except ImportError:
    from core import (
        BlockingEngine,
        DSU,
        agg_set,
        name_similarity,
        norm_phone,
        norm_text,
        strip_hco_name_affixes,
        resolve_hco_column,
        is_acronym_of,
        compute_acronym,
    )

logger = logging.getLogger("dupe_tool.hco")


# ── Canonicalization ─────────────────────────────────────────────────

def _build_hco_canonical(
    df: pd.DataFrame, cfg: dict[str, Any],
    progress_fn=None,
) -> pd.DataFrame:
    """Collapse multi-row-per-HCO-VID extract into one canonical record per HCO VID.

    Vectorized: pre-normalize columns in bulk before groupby.
    """

    cols = cfg["columns"]

    def _resolve_col(key: str) -> str:
        """Resolve a logical HCO field to an actual df column using centralized aliases + fuzzy fallback."""
        matched = resolve_hco_column(list(df.columns), key, cols)
        if matched:
            return matched
        # Fall back to config primary (may be missing — handled downstream)
        return cols.get(key, "")

    vid_col = _resolve_col("hco_entity_vid")
    name_col = _resolve_col("hco_name")
    type_col = _resolve_col("hco_type")
    phone_col = _resolve_col("hco_phone")
    fax_col = _resolve_col("hco_fax")
    city_col = _resolve_col("hco_city")
    state_col = _resolve_col("hco_state")
    postal_col = _resolve_col("hco_postal")
    addr1_col = _resolve_col("hco_addr_line1")
    addr2_col = _resolve_col("hco_addr_line2")
    status_col = _resolve_col("hco_status")

    # Check required columns
    if not vid_col or vid_col not in df.columns:
        logger.warning("HCO VID column '%s' not found — skipping HCO pipeline.", vid_col)
        return pd.DataFrame()
    if not name_col or name_col not in df.columns:
        logger.warning("HCO Name column '%s' not found — skipping HCO pipeline.", name_col)
        return pd.DataFrame()

    def _safe_col(col):
        return col if col and col in df.columns else None

    def _report(msg):
        if progress_fn:
            progress_fn(msg)
        logger.info(msg)

    # Profile-type-specific name normalization
    profile_type = cfg.get("profile_type", "hco")
    profile_cfg = cfg.get("profile_types", {}).get(profile_type, {})
    _strip_prefixes = profile_cfg.get("strip_prefixes", [])
    _strip_suffixes = profile_cfg.get("strip_suffixes", [])

    _report("Normalizing HCO columns (vectorized)...")

    # Pre-normalize all columns in bulk before groupby
    df = df.copy()
    df["_hco_name_norm"] = df[name_col].apply(norm_text)
    if _strip_prefixes or _strip_suffixes:
        _report(f"Applying {profile_type} name normalization (stripping {len(_strip_prefixes)} prefixes, {len(_strip_suffixes)} suffixes)...")
        df["_hco_name_norm"] = df["_hco_name_norm"].apply(
            lambda n: strip_hco_name_affixes(n, _strip_prefixes, _strip_suffixes)
        )
    if _safe_col(type_col):
        df["_hco_type_norm"] = df[type_col].apply(norm_text)
    if _safe_col(phone_col):
        df["_hco_phone_norm"] = df[phone_col].apply(norm_phone)
    if _safe_col(fax_col):
        df["_hco_fax_norm"] = df[fax_col].apply(norm_phone)
    if _safe_col(city_col):
        df["_hco_city_norm"] = df[city_col].apply(norm_text)
    if _safe_col(state_col):
        df["_hco_state_norm"] = df[state_col].apply(norm_text)
    if _safe_col(postal_col):
        df["_hco_postal_norm"] = df[postal_col].apply(
            lambda x: "".join(ch for ch in str(x) if ch.isdigit()) if not pd.isna(x) else ""
        )
    if _safe_col(addr1_col):
        df["_hco_addr1_norm"] = df[addr1_col].apply(norm_text)
    if _safe_col(addr2_col):
        df["_hco_addr2_norm"] = df[addr2_col].apply(norm_text)

    _report("Grouping HCO records by VID...")

    grouped = df.groupby(vid_col, sort=False)
    total_groups = len(grouped)
    progress_interval = max(1, total_groups // 20)
    records: list[dict[str, Any]] = []

    for group_idx, (vid, g) in enumerate(grouped):
        if pd.isna(vid) or not str(vid).strip():
            continue

        if progress_fn and group_idx % progress_interval == 0:
            _report(f"Canonicalizing HCO VIDs... {group_idx:,}/{total_groups:,}")

        # Name (use pre-normalized)
        raw_name = str(g[name_col].iloc[0]) if not pd.isna(g[name_col].iloc[0]) else ""
        name_norm = g["_hco_name_norm"].iloc[0]
        # Strip locality BEFORE normalization so the " - " separator is still intact
        # norm_text() converts " - " → space, so _strip_locality on name_norm never fires.
        raw_name_for_cmp = raw_name.split(" - ")[0].strip() if " - " in raw_name else raw_name
        name_stripped_norm = norm_text(raw_name_for_cmp)

        # Type (use pre-normalized)
        raw_type = ""
        type_norm = ""
        if _safe_col(type_col):
            raw_type = str(g[type_col].iloc[0]) if not pd.isna(g[type_col].iloc[0]) else ""
            type_norm = g["_hco_type_norm"].iloc[0]

        # Phone / Fax (use pre-normalized)
        phones: set[str] = set()
        faxes: set[str] = set()
        if _safe_col(phone_col):
            for v in g["_hco_phone_norm"]:
                if v is not None:
                    phones.add(v)
        if _safe_col(fax_col):
            for v in g["_hco_fax_norm"]:
                if v is not None:
                    faxes.add(v)

        # Address (use pre-normalized)
        city_norm = g["_hco_city_norm"].iloc[0] if _safe_col(city_col) else ""
        state_norm = g["_hco_state_norm"].iloc[0] if _safe_col(state_col) else ""
        postal_norm = g["_hco_postal_norm"].iloc[0] if _safe_col(postal_col) else ""
        addr_norm = ""
        if _safe_col(addr1_col):
            addr_norm = g["_hco_addr1_norm"].iloc[0]
            if _safe_col(addr2_col):
                a2 = g["_hco_addr2_norm"].iloc[0]
                if a2:
                    addr_norm = f"{addr_norm} {a2}".strip()

        # Status
        hco_status = ""
        if _safe_col(status_col):
            hco_status = str(g[status_col].iloc[0]) if not pd.isna(g[status_col].iloc[0]) else ""

        records.append({
            "vid": str(vid).strip(),
            "name_norm": name_norm,
            "name_stripped_norm": name_stripped_norm,
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

# Common Indian HCO name template words that carry no discriminating power.
# Stripping these before similarity comparison prevents "Dr X Clinic" from
# falsely matching "Dr Y Clinic" just because they share "dr" and "clinic".
_HCO_PREFIX_TEMPLATES = {
    "dr", "dr.", "prof", "professor", "mr", "mrs", "ms", "smt", "shri", "sri",
    "m s", "ms", "messrs",
}
_HCO_SUFFIX_TEMPLATES = {
    "clinic", "clinics",
    "hospital", "hospitals",
    "centre", "center",
    "nursing home",
    "dispensary",
    "polyclinic",
    "multispeciality", "multispecialty",
    "super speciality", "super specialty",
    "speciality hospital", "specialty hospital",
    "medical centre", "medical center",
    "health centre", "health center",
    "eye hospital", "eye care",
    "diagnostic centre", "diagnostic center",
    "maternity home",
    "eye clinic",
}


def _strip_hco_templates(name_norm: str) -> str:
    """Strip common Indian HCO honorific prefixes and generic suffixes.

    Applied before name similarity computation so that the differentiating
    part of the name (the actual identifier) drives the score.

    E.g.:
      "dr s k mehrotra s clinic"  → "s k mehrotra s"
      "dr abu obadiah s clinic"   → "abu obadiah s"
      "apollo hospital"           → "apollo"
      "shatabdi super speciality hospital" → "shatabdi"
    """
    if not name_norm:
        return name_norm

    # Strip honorific prefix (single token)
    tokens = name_norm.split()
    if tokens and tokens[0] in _HCO_PREFIX_TEMPLATES:
        tokens = tokens[1:]

    name_norm = " ".join(tokens)

    # Strip suffix phrases (longest match first)
    sorted_suffixes = sorted(_HCO_SUFFIX_TEMPLATES, key=len, reverse=True)
    changed = True
    while changed:
        changed = False
        for sfx in sorted_suffixes:
            if name_norm.endswith(" " + sfx) or name_norm == sfx:
                name_norm = name_norm[: -(len(sfx))].strip()
                changed = True
                break

    return name_norm.strip() or name_norm  # fallback if everything stripped


def _strip_locality(name_norm: str) -> str:
    """Strip Veeva locality suffix from HCO name.

    Veeva India naming convention: "hco name - locality"
    E.g., "apollo hospital - bhatinda" → "apollo hospital"
    Keeps the full name if no " - " separator found.
    """
    if " - " in name_norm:
        return name_norm.split(" - ")[0].strip()
    return name_norm


# Indian address tokens that are common landmarks/road-types but
# carry no discriminating power between different HCOs on the same street.
_INDIAN_ADDR_STOP = {
    # Road / street types
    "marg", "salai", "road", "rd", "street", "st", "lane", "ln",
    "nagar", "nagara", "nagar", "vihar", "colony", "enclave",
    "sector", "phase", "block", "zone", "extension", "extn",
    "bazaar", "bazar", "market", "mkt", "chowk", "chawk",
    "ganj", "gunj", "peth", "wadi", "wada",
    # Directional / generic
    "main", "new", "old", "east", "west", "north", "south",
    "no", "number", "plot", "flat", "floor", "fl",
    "building", "bldg", "complex", "tower", "mall",
    "suite", "ste", "unit", "wing",
    # Common proper-noun prefixes on roads (too widespread to be discriminating)
    "anna", "gandhi", "nehru", "mg", "jl", "nehru", "rajaji",
    "mahatma", "subhash", "netaji", "sardar", "indira",
}


def _compute_hco_signals(ar: dict, br: dict) -> dict[str, Any]:
    """Compute all match signals between two canonical HCO records.

    Name comparison uses locality-stripped names (Veeva convention: "HCO - Locality").
    Address is the primary gate; name is a confirmation signal.
    """

    # Step 1: strip locality suffix  ("Apollo Hospital - Mumbai" → "Apollo Hospital")
    # Use pre-stripped version if available (stored in canonical record, where stripping
    # is done on the raw name before norm_text converts " - " to a space).
    a_name_stripped = ar.get("name_stripped_norm") or _strip_locality(ar["name_norm"])
    b_name_stripped = br.get("name_stripped_norm") or _strip_locality(br["name_norm"])

    # Step 2: strip HCO template words ("Apollo Hospital" → "Apollo")
    # This prevents "Dr X Clinic" matching "Dr Y Clinic" via shared "dr"/"clinic" tokens
    a_name_core = _strip_hco_templates(a_name_stripped)
    b_name_core = _strip_hco_templates(b_name_stripped)

    nscore = name_similarity(a_name_core, b_name_core)

    phone_ov = set(ar["phones"]) & set(br["phones"])
    fax_ov = set(ar["faxes"]) & set(br["faxes"])

    type_match = bool(ar["type_norm"] and ar["type_norm"] == br["type_norm"])
    type_diff = bool(ar["type_norm"] and br["type_norm"] and ar["type_norm"] != br["type_norm"])

    city_match = bool(ar["city_norm"] and ar["city_norm"] == br["city_norm"])
    city_diff = bool(ar["city_norm"] and br["city_norm"] and ar["city_norm"] != br["city_norm"])
    state_match = bool(ar["state_norm"] and ar["state_norm"] == br["state_norm"])
    postal_match = bool(ar["postal_norm"] and ar["postal_norm"] == br["postal_norm"])

    # Address overlap — uses Indian-aware stop word list, tightened to 70% token overlap
    addr_overlap = False
    if ar["addr_norm"] and br["addr_norm"]:
        a_tokens = set(ar["addr_norm"].split()) - _INDIAN_ADDR_STOP
        b_tokens = set(br["addr_norm"].split()) - _INDIAN_ADDR_STOP
        if a_tokens and b_tokens and min(len(a_tokens), len(b_tokens)) >= 2:
            overlap_ratio = len(a_tokens & b_tokens) / min(len(a_tokens), len(b_tokens))
            addr_overlap = overlap_ratio >= 0.70  # tightened from 0.50

    address_match = (postal_match and city_match) or (addr_overlap and city_match)

    # Acronym detection — use template-stripped core names
    # "dr" is now in _ACRONYM_STOPWORDS so it won't fire as a false acronym
    a_first = a_name_core.split()[0] if a_name_core.split() else ""
    b_first = b_name_core.split()[0] if b_name_core.split() else ""
    acronym_match = (
        is_acronym_of(a_first, b_name_core) or
        is_acronym_of(b_first, a_name_core)
    )

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
        "acronym_match": acronym_match,
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

    # H4: Acronym + Address + Same type  (e.g., "GSV" = "Ganesh Siddha Venkateswara")
    if sig.get("acronym_match") and sig["address_overlap"] and sig["type_match"]:
        return "H4_ACRONYM_ADDR_TYPE", f"Acronym match + Address + Same type (name sim: {ns:.0f}%)"

    # H5: Acronym + Address + Phone/Fax
    if sig.get("acronym_match") and sig["address_overlap"] and sig["phone_or_fax_overlap"]:
        return "H5_ACRONYM_ADDR_PHONE", f"Acronym match + Address + Phone/Fax (name sim: {ns:.0f}%)"

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
    if sig.get("acronym_match"):
        score += w.get("acronym_match", 20)
        reasons.append("ACRONYM_MATCH")

    # Negatives
    if sig["type_different"]:
        score += w.get("different_type", -20)
        reasons.append("DIFF_TYPE")
    if sig["city_different"]:
        score += w.get("different_city", -15)
        reasons.append("DIFF_CITY")

    # Gate 1 — Address is mandatory: different address = cannot be duplicate
    if not sig["address_overlap"]:
        return 0, [], "No address overlap — not a candidate duplicate"

    # Gate 2 — Name is mandatory: must have some name similarity OR acronym match
    # Prevents shared-building false positives (e.g., Endo Kids Clinic vs Ortho Kids Clinic)
    has_name_signal = ns >= 75 or sig.get("acronym_match")
    if not has_name_signal:
        return 0, [], "No name signal — different HCOs sharing same address/postal area"

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

def _enrich_hco_row(vid_a: str, vid_b: str, canon_idx) -> dict:
    """Pull display columns for both VIDs. Accepts dict or DataFrame index."""
    if isinstance(canon_idx, dict):
        ar = canon_idx.get(vid_a, {})
        br = canon_idx.get(vid_b, {})
        _get = lambda d, k: d.get(k, "")
    else:
        ar = canon_idx.loc[vid_a]
        br = canon_idx.loc[vid_b]
        _get = lambda d, k: d[k]
    return {
        "name_a": _get(ar, "display_name"),
        "name_b": _get(br, "display_name"),
        "type_a": _get(ar, "display_type"),
        "type_b": _get(br, "display_type"),
        "city_a": _get(ar, "display_city"),
        "city_b": _get(br, "display_city"),
        "phones_a": _get(ar, "display_phones"),
        "phones_b": _get(br, "display_phones"),
    }


# ── Main Pipeline ────────────────────────────────────────────────────

def run_hco_pipeline(
    df: pd.DataFrame, cfg: dict[str, Any],
    progress_fn=None,
) -> dict[str, pd.DataFrame]:
    """Execute the full HCO dedup pipeline and return result DataFrames."""

    enrich = cfg.get("output", {}).get("enrich_output", True)
    review_threshold = cfg.get("hco_review_scoring", {}).get("review_threshold", 50)
    max_pairs = cfg.get("blocking", {}).get("max_pairs", 0)

    # Apply profile-specific rule/scoring overrides
    profile_type = cfg.get("profile_type", "hco")
    profile_cfg = cfg.get("profile_types", {}).get(profile_type, {})
    working_cfg = cfg

    rule_overrides = profile_cfg.get("hco_auto_rule_overrides", {})
    scoring_overrides = profile_cfg.get("review_scoring_overrides", {})
    if rule_overrides or scoring_overrides:
        working_cfg = dict(cfg)
        if rule_overrides:
            merged_rules = dict(cfg.get("hco_auto_rules", {}))
            for rule_name, overrides in rule_overrides.items():
                if rule_name in merged_rules:
                    merged_rules[rule_name] = {**merged_rules[rule_name], **overrides}
                else:
                    merged_rules[rule_name] = overrides
            working_cfg["hco_auto_rules"] = merged_rules
        if scoring_overrides:
            merged_scoring = dict(cfg.get("hco_review_scoring", {}))
            merged_scoring.update(scoring_overrides)
            working_cfg["hco_review_scoring"] = merged_scoring
            review_threshold = merged_scoring.get("review_threshold", review_threshold)

    def _progress(pct, msg):
        if progress_fn:
            progress_fn(pct, msg)
        logger.info(msg)

    # Step 1 — Canonicalize
    _progress(91, "Canonicalizing HCO records...")
    def _canon_progress(msg):
        _progress(91, msg)
    canon = _build_hco_canonical(df, cfg, progress_fn=_canon_progress)
    if canon.empty:
        logger.warning("No HCO records found — skipping HCO pipeline.")
        return {
            "hco_canonical": pd.DataFrame(),
            "hco_auto": pd.DataFrame(),
            "hco_review": pd.DataFrame(),
            "hco_notdup": pd.DataFrame(),
            "hco_unique": pd.DataFrame(),
            "hco_clusters": pd.DataFrame(),
            "hco_summary": pd.DataFrame(),
        }

    # Build dict-based index for O(1) lookups (avoid repeated .loc[] on DataFrame)
    canon_dict: dict[str, dict] = {}
    for _, row in canon.iterrows():
        canon_dict[row["vid"]] = row.to_dict()
    canon_idx = canon.set_index("vid", drop=False)

    # Step 2 — Blocking
    _progress(93, "Building HCO blocking keys...")
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=False,  # Not applicable for HCOs
    )
    for rec in canon_dict.values():
        blocker.add_hco(rec["vid"], rec)
    pairs = blocker.candidate_pairs(max_pairs=max_pairs)

    # Step 3 — Classify pairs
    _progress(94, f"Evaluating {len(pairs):,} HCO candidate pairs...")
    auto_rows: list[dict] = []
    review_rows: list[dict] = []
    notdup_rows: list[dict] = []

    total_pairs = len(pairs)
    progress_interval = max(1, total_pairs // 10)

    for pair_idx, (a, b) in enumerate(sorted(pairs)):
        if progress_fn and pair_idx % progress_interval == 0:
            pct = 94 + int(4 * pair_idx / max(total_pairs, 1))
            _progress(pct, f"Evaluating HCO pairs... {pair_idx:,}/{total_pairs:,}")

        ar = canon_dict[a]
        br = canon_dict[b]
        sig = _compute_hco_signals(ar, br)

        base = {
            "vid_a": a,
            "vid_b": b,
            "name_similarity": round(sig["nscore"], 1),
            "acronym_match": int(sig.get("acronym_match", False)),
            "type_match": int(sig["type_match"]),
            "city_match": int(sig["city_match"]),
            "postal_match": int(sig["postal_match"]),
            "address_match": int(sig["address_overlap"]),
            "phone_fax_match": int(sig["phone_or_fax_overlap"]),
        }
        if enrich:
            base.update(_enrich_hco_row(a, b, canon_dict))

        # AUTO
        auto_result = _apply_hco_auto_rules(sig, working_cfg)
        if auto_result:
            rule, comment = auto_result
            base["rule"] = rule
            base["comments"] = comment
            auto_rows.append(base)
            continue

        # Score for REVIEW
        score, reasons, comment = _score_hco_review(sig, ar, br, working_cfg)
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

    # ── UNIQUE VIDs (no candidate pairs at all) ──────────────────────
    all_canon_vids = set(canon["vid"])
    paired_vids: set[str] = set()
    for frame in (auto_df, review_df, notdup_df):
        if not frame.empty and "vid_a" in frame.columns:
            paired_vids |= set(frame["vid_a"].astype(str))
            paired_vids |= set(frame["vid_b"].astype(str))
    unique_vids = all_canon_vids - paired_vids

    unique_rows = []
    for vid in sorted(unique_vids):
        row = {"vid": vid, "status": "UNIQUE",
               "comments": "No candidate pairs found — confirmed unique"}
        if enrich and vid in canon_idx.index:
            r = canon_idx.loc[vid]
            row["name"] = r["display_name"]
            row["type"] = r.get("display_type", "")
            row["city"] = r.get("display_city", "")
        unique_rows.append(row)
    unique_df = pd.DataFrame(unique_rows) if unique_rows else pd.DataFrame()

    # Effort Avoidance metrics (VID-based, not pair-based)
    velocity = cfg.get("manual_review", {}).get("pairs_per_hour", 6.5)

    # Count unique VIDs across auto-merge pairs (industry standard: records, not pairs)
    if not auto_df.empty and "vid_a" in auto_df.columns and "vid_b" in auto_df.columns:
        auto_merge_vids = len(set(auto_df["vid_a"]) | set(auto_df["vid_b"]))
    else:
        auto_merge_vids = 0

    # Count unique VIDs in review pairs (still need manual review)
    if not review_df.empty and "vid_a" in review_df.columns and "vid_b" in review_df.columns:
        review_vids = len(set(review_df["vid_a"]) | set(review_df["vid_b"]))
    else:
        review_vids = 0

    # Count unique VIDs in NOT_DUP pairs
    if not notdup_df.empty and "vid_a" in notdup_df.columns and "vid_b" in notdup_df.columns:
        notdup_vids = len(set(notdup_df["vid_a"]) | set(notdup_df["vid_b"]))
    else:
        notdup_vids = 0

    hours_saved = round(auto_merge_vids / velocity, 1) if velocity else 0
    remaining_hours = round(review_vids / velocity, 1) if velocity else 0

    summary = {
        "profile_type": profile_type,
        "unique_hco_vids": len(canon),
        "candidate_pairs_evaluated": len(pairs),
        "auto_merge_pairs": len(auto_df),
        "auto_merge_vids": auto_merge_vids,
        "review_pairs": len(review_df),
        "review_vids": review_vids,
        "not_dup_pairs": len(notdup_df),
        "not_dup_vids": notdup_vids,
        "unique_vids": len(unique_vids),
        "auto_clusters": cluster_df["cluster_id"].nunique() if not cluster_df.empty else 0,
        "vids_auto_resolved": auto_merge_vids,
        "manual_velocity_per_hr": velocity,
        "effort_avoidance_hours": hours_saved,
        "est_remaining_review_hours": remaining_hours,
    }

    logger.info(
        "HCO Pipeline complete: %d AUTO, %d REVIEW, %d NOT_DUP, %d UNIQUE, %d clusters",
        summary["auto_merge_pairs"],
        summary["review_pairs"],
        summary["not_dup_pairs"],
        summary["unique_vids"],
        summary["auto_clusters"],
    )

    return {
        "hco_canonical": canon,
        "hco_auto": auto_df,
        "hco_review": review_df,
        "hco_notdup": notdup_df,
        "hco_unique": unique_df,
        "hco_clusters": cluster_df,
        "hco_summary": pd.DataFrame([summary]),
    }


# ── Cross-Dataset Pipeline (Target vs Master) ────────────────────────

def run_hco_cross_pipeline(
    target_canon: pd.DataFrame,
    master_df: pd.DataFrame,
    cfg: dict[str, Any],
    progress_fn=None,
) -> dict[str, pd.DataFrame]:
    """Compare Target HCOs against the Master Data universe.

    Builds a blocking index from Master Data, probes each Target record,
    and returns only pairs where target_vid ≠ master_vid and master_vid
    is NOT in the target set (i.e., potential dupes in the broader universe).
    """

    def _progress(msg: str) -> None:
        if progress_fn:
            progress_fn(msg)
        logger.info(msg)

    _progress("Canonicalizing Master Data for cross-comparison...")
    master_canon = _build_hco_canonical(master_df, cfg)
    if master_canon.empty:
        logger.warning("Master Data canonicalization returned empty — aborting cross-pipeline.")
        return {"cross_auto": pd.DataFrame(), "cross_review": pd.DataFrame(), "cross_notdup": pd.DataFrame()}

    target_vid_set = set(target_canon["vid"])

    # Build blocking index from master
    _progress(f"Building cross-blocking index from {len(master_canon):,} Master Data VIDs...")
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=False,
    )
    master_dict: dict[str, dict] = {}
    for _, row in master_canon.iterrows():
        rec = row.to_dict()
        master_dict[rec["vid"]] = rec
        blocker.add_hco(rec["vid"], rec)

    target_dict: dict[str, dict] = {row["vid"]: row.to_dict() for _, row in target_canon.iterrows()}

    # Probe each target record against master index
    _progress(f"Probing {len(target_dict):,} Target VIDs against Master Data index...")
    cross_pairs: set[tuple[str, str]] = set()
    for t_vid, t_rec in target_dict.items():
        candidates = blocker.probe_hco(t_rec)
        for m_vid in candidates:
            if m_vid == t_vid:
                continue
            if m_vid in target_vid_set:
                continue  # Both in target — handled by within-target run
            a, b = (t_vid, m_vid) if t_vid < m_vid else (m_vid, t_vid)
            cross_pairs.add((a, b))

    _progress(f"Evaluating {len(cross_pairs):,} cross candidate pairs...")
    enrich = cfg.get("output", {}).get("enrich_output", True)
    review_threshold = cfg.get("hco_review_scoring", {}).get("review_threshold", 50)

    auto_rows: list[dict] = []
    review_rows: list[dict] = []
    notdup_rows: list[dict] = []

    for a, b in sorted(cross_pairs):
        # Identify which VID is target and which is master
        if a in target_dict:
            t_vid, m_vid, ar, br = a, b, target_dict[a], master_dict.get(b, {})
        else:
            t_vid, m_vid, ar, br = b, a, target_dict[b], master_dict.get(a, {})

        if not br:
            continue

        sig = _compute_hco_signals(ar, br)

        base: dict[str, Any] = {
            "target_vid": t_vid,
            "master_vid": m_vid,
            "name_similarity": round(sig["nscore"], 1),
            "acronym_match": int(sig.get("acronym_match", False)),
            "type_match": int(sig["type_match"]),
            "city_match": int(sig["city_match"]),
            "postal_match": int(sig["postal_match"]),
            "address_match": int(sig["address_overlap"]),
            "phone_fax_match": int(sig["phone_or_fax_overlap"]),
        }
        if enrich:
            base["target_name"] = ar.get("display_name", "")
            base["master_name"] = br.get("display_name", "")
            base["target_city"] = ar.get("display_city", "")
            base["master_city"] = br.get("display_city", "")
            base["target_type"] = ar.get("display_type", "")
            base["master_type"] = br.get("display_type", "")

        auto_result = _apply_hco_auto_rules(sig, cfg)
        if auto_result:
            rule, comment = auto_result
            base["rule"] = rule
            base["comments"] = comment
            auto_rows.append(base)
            continue

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

    cross_review_df = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()
    if not cross_review_df.empty and "score" in cross_review_df.columns:
        cross_review_df = cross_review_df.sort_values("score", ascending=False).reset_index(drop=True)

    _progress(
        f"Cross-pipeline complete: {len(auto_rows)} AUTO, "
        f"{len(review_rows)} REVIEW, {len(notdup_rows)} NOT_DUP"
    )

    return {
        "cross_auto": pd.DataFrame(auto_rows) if auto_rows else pd.DataFrame(),
        "cross_review": cross_review_df,
        "cross_notdup": pd.DataFrame(notdup_rows) if notdup_rows else pd.DataFrame(),
    }

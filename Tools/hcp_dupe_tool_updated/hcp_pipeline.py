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

try:
    from .core import (
        BlockingEngine,
        DSU,
        NameSimilarityCache,
        SharedContactDetector,
        SpecialtySynonymResolver,
        agg_set,
        name_similarity,
        norm_email,
        norm_license,
        norm_phone,
        norm_text,
        strip_name_suffixes,
    )
    from .common_names import IndianCommonNameDetector
except ImportError:
    from core import (
        BlockingEngine,
        DSU,
        NameSimilarityCache,
        SharedContactDetector,
        SpecialtySynonymResolver,
        agg_set,
        name_similarity,
        norm_email,
        norm_license,
        norm_phone,
        norm_text,
        strip_name_suffixes,
    )
    from common_names import IndianCommonNameDetector

logger = logging.getLogger("dupe_tool.hcp")


# ── Canonicalization ─────────────────────────────────────────────────

def _build_canonical(
    df: pd.DataFrame, cfg: dict[str, Any],
    progress_fn=None,
) -> pd.DataFrame:
    """Collapse multi-row-per-VID extract into one canonical record per VID.

    Vectorized approach: pre-normalize all columns in bulk using pandas
    vectorized ops, then aggregate per VID using optimized groupby.
    """

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

    def _report(msg):
        if progress_fn:
            progress_fn(msg)
        logger.info(msg)

    _report("Normalizing columns (vectorized)...")

    # Pre-compute normalized columns using vectorized apply (bulk, not per-group)
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

    # Pre-normalize phone/email columns in bulk
    for pc in phone_cols:
        df[f"_pn_{pc}"] = df[pc].apply(norm_phone)
    for ec in email_cols:
        df[f"_em_{ec}"] = df[ec].apply(norm_email)
    # Pre-normalize specialty columns
    for sc in spec_cols:
        df[f"_sp_{sc}"] = df[sc].apply(norm_text)
    # Pre-normalize HCO columns
    for hc in hco_cols:
        df[f"_hco_{hc}"] = df[hc].apply(norm_text)
    # Pre-normalize license columns
    if has_license:
        df["_lic_num_norm"] = df[lic_num_col].apply(norm_license)
        df["_lic_body_norm"] = df[lic_body_col].apply(norm_text)
        df["_lic_stat_upper"] = df[lic_stat_col].fillna("").astype(str).str.strip().str.upper()

    _report("Grouping by VID...")

    # Group and build records — use pre-normalized columns
    grouped = df.groupby(vid_col, sort=False)
    total_groups = len(grouped)
    records: list[dict[str, Any]] = []
    progress_interval = max(1, total_groups // 20)

    for group_idx, (vid, g) in enumerate(grouped):
        if progress_fn and group_idx % progress_interval == 0:
            _report(f"Canonicalizing VIDs... {group_idx:,}/{total_groups:,}")

        # ── Name (use pre-normalized columns) ───
        first_v = g["_first_norm"].iloc[0] if "_first_norm" in g.columns and isinstance(g["_first_norm"].iloc[0], str) else ""
        mid_v = g["_mid_norm"].iloc[0] if "_mid_norm" in g.columns and isinstance(g["_mid_norm"].iloc[0], str) else ""
        last_v = g["_last_norm"].iloc[0]

        parts = [p for p in (first_v, mid_v, last_v) if p]
        name_canon = " ".join(parts).strip()
        if not name_canon and full_col and full_col in df.columns:
            name_canon = norm_text(g[full_col].iloc[0])
        if suffixes:
            name_canon = strip_name_suffixes(name_canon, suffixes)

        first_name_norm = first_v
        last_name_norm = last_v

        # ── Contacts (use pre-normalized columns) ──
        phones: set[str] = set()
        for pc in phone_cols:
            col_key = f"_pn_{pc}"
            for v in g[col_key]:
                if v is not None:
                    phones.add(v)
        emails: set[str] = set()
        for ec in email_cols:
            col_key = f"_em_{ec}"
            for v in g[col_key]:
                if v is not None:
                    emails.add(v)

        # ── Licenses (use pre-normalized columns) ──
        lic_tuples: set[tuple[str, str]] = set()
        active_lic_tuples: set[tuple[str, str]] = set()
        if has_license:
            ln_arr = g["_lic_num_norm"].values
            lb_arr = g["_lic_body_norm"].values
            ls_arr = g["_lic_stat_upper"].values
            for i in range(len(ln_arr)):
                ln_val = ln_arr[i]
                lb_val = lb_arr[i]
                if not ln_val and not lb_val:
                    continue
                t = (ln_val or "", lb_val or "")
                lic_tuples.add(t)
                if ls_arr[i] in ("A", "ACTIVE"):
                    active_lic_tuples.add(t)

        # ── HCO VIDs (use pre-normalized columns) ──
        hco_vids: set[str] = set()
        for hc in hco_cols:
            col_key = f"_hco_{hc}"
            for val in g[col_key]:
                if val:
                    hco_vids.add(val)

        # ── Geo ─────────────────────────────────
        pins = set(g["_pin_norm"].astype(str).str.strip()) - {"", "0"}
        cities = set(g["_city_cda_norm"].astype(str).str.strip())
        cities |= set(g["_addr_city_norm"].astype(str).str.strip())
        cities.discard("")
        states = set(g["_addr_state_norm"].astype(str).str.strip())
        states.discard("")
        city_cda_val = g["_city_cda_norm"].iloc[0] if "_city_cda_norm" in g.columns else ""

        # ── Specialties (use pre-normalized columns) ──
        specialties: set[str] = set()
        for sc in spec_cols:
            col_key = f"_sp_{sc}"
            for val in g[col_key]:
                if val:
                    specialties.add(val)

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
    ar: dict, br: dict,
    shared_detector: SharedContactDetector | None = None,
    name_cache: NameSimilarityCache | None = None,
    spec_resolver: SpecialtySynonymResolver | None = None,
) -> dict[str, Any]:
    """Compute all match signals between two canonical HCP records.

    Args:
        ar: Record A as dict (works with both Series and plain dicts)
        br: Record B as dict
        shared_detector: SharedContactDetector instance (required for accurate shared contact detection)
        name_cache: Optional NameSimilarityCache for large-dataset performance
        spec_resolver: Optional SpecialtySynonymResolver for synonym-aware matching

    Returns:
        Dictionary of signal computations
    """

    phones_ov = set(ar["phones"]) & set(br["phones"])
    emails_ov = set(ar["emails"]) & set(br["emails"])

    # Specialty overlap — resolve synonyms before comparing
    a_specs = set(ar["specialties"])
    b_specs = set(br["specialties"])
    if spec_resolver and spec_resolver.enabled:
        a_resolved = spec_resolver.resolve_set(a_specs)
        b_resolved = spec_resolver.resolve_set(b_specs)
        spec_ov = a_resolved & b_resolved
    else:
        spec_ov = a_specs & b_specs
    cities_ov = set(ar["cities"]) & set(br["cities"])
    pins_ov = set(ar["pins"]) & set(br["pins"])
    states_ov = set(ar["states"]) & set(br["states"])
    hco_ov = set(ar["hco_vids"]) & set(br["hco_vids"])

    if name_cache:
        nscore = name_cache.get(ar["name_canon"], br["name_canon"])
    else:
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
        "states_overlap": states_ov,
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

def _apply_auto_rules(
    sig: dict, cfg: dict,
    ar: dict | None = None, br: dict | None = None,
    cn_detector: IndianCommonNameDetector | None = None,
) -> tuple[str, str, str] | None:
    """Check AUTO-MERGE rules in priority order.

    Returns:
        (rule_code, comment, rationale) or None.
        The rationale is a human-readable sentence for leadership reporting.
    """

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
        rat = (
            f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
            f"and shared HCO affiliation confirm duplicate."
        )
        return "G1_NAME_SPL_HCO", cmt, rat

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
        rat = (
            f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
            f"and postal code ({pins_str}) confirm duplicate."
        )
        return "G2_NAME_SPL_PIN", cmt, rat

    # ── G3: Name + Specialty + City — ENHANCED with common-name awareness ──
    r = rules.get("G3_name_spec_city", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["spec_overlap"]
        and sig["cities_overlap"]
    ):
        cities_str = ",".join(sorted(sig["cities_overlap"])[:3])

        # G3a: Name + Spec + City + Phone → always auto (strongest corroboration)
        if sig["phones_overlap"] and not sig["shared_contact"]:
            ph_str = ";".join(sorted(sig["phones_overlap"]))[:40]
            cmt = (
                f"Name + Specialty + City + Phone match "
                f"(Spec: {spec_str}, City: {cities_str}, Ph: {ph_str})"
            )
            rat = (
                f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
                f"same city ({cities_str}), and phone number matches — "
                f"confirmed duplicate."
            )
            return "G3a_NAME_SPL_CITY_PHONE", cmt, rat

        # G3b: Name + Spec + City + License → always auto
        if sig["active_license_match"]:
            cmt = (
                f"Name + Specialty + City + License match "
                f"(Spec: {spec_str}, City: {cities_str}, Lic: {sig['license_info']})"
            )
            rat = (
                f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
                f"same city ({cities_str}), and license number matches — "
                f"confirmed duplicate."
            )
            return "G3b_NAME_SPL_CITY_LICENSE", cmt, rat

        # G3c / G3d: Check name commonality
        if cn_detector and ar and br:
            fn_a = ar.get("first_name_norm", "")
            ln_a = ar.get("last_name_norm", "")
            cn_result = cn_detector.classify(fn_a, ln_a)

            if not cn_result["is_common"]:
                # G3c: Uncommon name → auto-merge
                display_name = ar.get("display_name", ar.get("name_canon", ""))
                cmt = (
                    f"Name + Specialty + City match "
                    f"(Spec: {spec_str}, City: {cities_str}) [UNCOMMON NAME]"
                )
                rat = (
                    f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
                    f"same city ({cities_str}). Name '{display_name}' is "
                    f"uncommon ({cn_result['reason']}) — confirmed duplicate."
                )
                return "G3c_NAME_SPL_CITY_UNCOMMON", cmt, rat
            # else: G3d — common name, NO early return.
            # Fall through to G4/G5/G6 so corroborating signals
            # (license, phone+email, email) can still auto-merge.
        else:
            # No detector available → legacy behaviour (auto-merge)
            cmt = f"Name + Specialty + City match (Spec: {spec_str}, City: {cities_str})"
            rat = (
                f"Name match ({ns:.0f}%), same specialty ({spec_str}), "
                f"same city ({cities_str}) — confirmed duplicate."
            )
            return "G3_NAME_SPL_CITY", cmt, rat

    # G4: Active license match + reasonable name
    r = rules.get("G4_license_match", {})
    if (
        ns >= r.get("name_min", 80)
        and sig["active_license_match"]
    ):
        cmt = f"Active license match + name similarity {ns:.0f}%"
        if sig["spec_overlap"]:
            cmt += f" [+Spec: {spec_str}]"
        rat = (
            f"Active license match with name similarity ({ns:.0f}%) "
            f"confirms duplicate."
        )
        return "G4_LICENSE", cmt, rat

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
        rat = (
            f"Name match ({ns:.0f}%), shared non-public phone and email "
            f"confirm duplicate."
        )
        return "G5_PHONE_EMAIL", cmt, rat

    # G6: Email + strong name (non-shared)
    r = rules.get("G6_email_name", {})
    if (
        ns >= r.get("name_min", 92)
        and sig["emails_overlap"]
        and not sig["shared_contact"]
    ):
        em_str = ";".join(sorted(sig["emails_overlap"]))[:80]
        cmt = f"Email + Strong name match (Em: {em_str})"
        rat = (
            f"Name match ({ns:.0f}%) and shared non-public email "
            f"confirm duplicate."
        )
        return "G6_EMAIL_NAME", cmt, rat

    return None


def _apply_not_dup_rules(sig: dict, ar: dict, br: dict, cfg: dict) -> tuple[str, str, str] | None:
    """Check NOT-DUPLICATE rules. Returns (rule_code, comment, rationale) or None."""

    ns = sig["nscore"]
    rules = cfg.get("hcp_not_dup_rules", {})

    # N1: Active license conflict
    r = rules.get("N1_active_license_conflict", {})
    if sig["active_license_conflict"]:
        # Exception: strong contact overlap
        if sig["phones_overlap"] and sig["emails_overlap"]:
            pass  # fall through
        else:
            cmt = "Both have active licenses but none match — different practitioners"
            rat = (
                "Both records hold active licenses but none match. "
                "Different license numbers indicate different practitioners."
            )
            return "N1_LICENSE_CONFLICT", cmt, rat

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
            cmt = f"Same name + specialty but different cities (A: {a_city}, B: {b_city})"
            rat = (
                f"Name and specialty match but records are in different cities "
                f"({a_city} vs {b_city}) — likely different practitioners."
            )
            return "N2_DIFFERENT_CITIES", cmt, rat

    return None


def _score_review(
    sig: dict, ar: dict, br: dict, cfg: dict,
    common_name_flag: bool = False,
    cn_detector: IndianCommonNameDetector | None = None,
) -> tuple[int, list[str], str, str]:
    """Compute a review score, comment, and human-readable rationale.

    Returns:
        (score, reasons_list, comment, rationale)
    """

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
        # Suppress specialty conflict penalty when HCO + city/geo both match
        # (same person can practice multiple specialties at the same HCO)
        if sig["hco_overlap"] and (sig["cities_overlap"] or sig["pins_overlap"] or sig["geo_support"]):
            reasons.append("SPEC_DIFF_HCO_OVERRIDE")
        else:
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
    elif sig.get("states_overlap"):
        # State-only match (no city overlap): only count when
        # name is uncommon OR name similarity >= 90%
        state_eligible = ns >= 90
        if not state_eligible and cn_detector:
            fn_a = ar.get("first_name_norm", "")
            ln_a = ar.get("last_name_norm", "")
            state_eligible = not cn_detector.is_common(fn_a, ln_a)
        if state_eligible:
            score += w.get("state_match", 15)
            reasons.append("STATE_MATCH")
        else:
            # Fall through to geo_support for common names below 90%
            if sig["geo_support"]:
                score += w.get("geo_support", 10)
                reasons.append("GEO_SUPPORT")
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

    # ── Common name flag ────────────────────
    if common_name_flag:
        reasons.append("COMMON_NAME")

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
    if common_name_flag:
        comment += " [COMMON NAME — manual verification recommended]"

    # ── Rationale (human-readable) ──────────
    parts = []
    parts.append(f"Name match ({ns:.0f}%)")
    if sig["spec_overlap"]:
        parts.append(f"same specialty ({','.join(sorted(sig['spec_overlap'])[:2])})")
    if sig["cities_overlap"]:
        parts.append(f"same city ({','.join(sorted(sig['cities_overlap'])[:2])})")
    elif sig.get("states_overlap"):
        parts.append(f"same state ({','.join(sorted(sig['states_overlap'])[:2])})")
    elif sig["pins_overlap"]:
        parts.append(f"same postal code")
    if sig["hco_overlap"]:
        parts.append("shared HCO affiliation")
    if sig["phones_overlap"] and not sig["shared_contact"]:
        parts.append("shared phone number")
    if sig["emails_overlap"] and not sig["shared_contact"]:
        parts.append("shared email")
    if sig["license_match"]:
        parts.append("license match")

    rationale = ", ".join(parts) + f". Score: {score}/100."

    if common_name_flag:
        display_name = ar.get("display_name", ar.get("name_canon", ""))
        rationale += (
            f" Name '{display_name}' is common in India — "
            f"manual verification recommended."
        )

    if sig["active_license_conflict"]:
        rationale += " Warning: active license conflict detected."

    return score, reasons, comment, rationale


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
    df: pd.DataFrame, cfg: dict[str, Any],
    progress_fn=None,
) -> dict[str, pd.DataFrame]:
    """Execute the full HCP dedup pipeline and return result DataFrames.

    Args:
        df: Input DataFrame (exploded Veeva extract).
        cfg: Configuration dict.
        progress_fn: Optional callable(pct: int, msg: str) for progress updates.
    """

    enrich = cfg.get("output", {}).get("enrich_output", True)
    max_contact = cfg.get("output", {}).get("max_contact_display", 200)
    review_threshold = cfg.get("hcp_review_scoring", {}).get("review_threshold", 55)
    max_pairs = cfg.get("blocking", {}).get("max_pairs", 0)

    def _progress(pct, msg):
        if progress_fn:
            progress_fn(pct, msg)
        logger.info(msg)

    # Step 1 — Canonicalize
    _progress(5, "Canonicalizing records...")
    def _canon_progress(msg):
        _progress(8, msg)
    canon = _build_canonical(df, cfg, progress_fn=_canon_progress)

    # Build a dict-based index for O(1) lookups (much faster than DataFrame.loc
    # for millions of pair evaluations)
    canon_dict: dict[str, dict] = {}
    for _, row in canon.iterrows():
        canon_dict[row["vid"]] = row.to_dict()
    # Keep DataFrame index for enrichment display
    canon_idx = canon.set_index("vid", drop=False)

    _progress(15, f"Canonicalized {len(canon):,} unique VIDs")

    # Step 2a — Shared contacts
    _progress(18, "Detecting shared contacts...")
    shared_det = SharedContactDetector(
        threshold=cfg.get("shared_contact", {}).get("threshold", 5)
    )
    for rec in canon_dict.values():
        shared_det.feed(set(rec["phones"]), set(rec["emails"]))
    shared_det.finalize()

    # Step 2b — Common name detector
    cn_cfg = cfg.get("common_name_detection", {})
    cn_detector = None
    if cn_cfg.get("enabled", True):
        cn_detector = IndianCommonNameDetector(cn_cfg)
    else:
        logger.info("Common name detection disabled")

    # Step 2c — Specialty synonym resolver
    spec_resolver = SpecialtySynonymResolver(cfg.get("specialty_synonyms"))
    if spec_resolver.enabled:
        logger.info("Specialty synonym resolver loaded")

    # Step 3 — Blocking
    _progress(20, "Building blocking keys...")
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=blk_cfg.get("first_initial_blocking", True),
        spec_resolver=spec_resolver,
    )
    for rec in canon_dict.values():
        blocker.add_hcp(rec["vid"], rec)

    _progress(25, "Generating candidate pairs...")
    pairs = blocker.candidate_pairs(max_pairs=max_pairs)

    # Step 4 — Classify pairs
    _progress(30, f"Evaluating {len(pairs):,} candidate pairs...")
    auto_rows: list[dict] = []
    review_rows: list[dict] = []
    notdup_rows: list[dict] = []
    cn_auto_promoted = 0   # G3a + G3b + G3c count
    cn_review_flagged = 0  # G3d (common name → review) count

    # Name similarity cache — avoids recomputing for the same name pair
    # (very beneficial for Indian datasets with many common names)
    nsim_cache = NameSimilarityCache(max_size=300_000)

    # Progress tracking for large pair sets
    total_pairs = len(pairs)
    progress_interval = max(1, total_pairs // 20)  # Update every 5%

    for pair_idx, (a, b) in enumerate(pairs):
        # Periodic progress update
        if progress_fn and pair_idx % progress_interval == 0:
            pct = 30 + int(55 * pair_idx / total_pairs)
            _progress(pct, f"Evaluating pairs... {pair_idx:,}/{total_pairs:,}")

        ar = canon_dict[a]
        br = canon_dict[b]
        sig = _compute_signals(ar, br, shared_det, name_cache=nsim_cache, spec_resolver=spec_resolver)

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
            base.update(_enrich_row(a, b, canon_dict))

        # Detect if this pair is a common-name G3 candidate that fell through
        is_common_name_city = False
        r3 = cfg.get("hcp_auto_rules", {}).get("G3_name_spec_city", {})
        if (
            cn_detector
            and sig["nscore"] >= r3.get("name_min", 92)
            and sig["spec_overlap"]
            and sig["cities_overlap"]
        ):
            fn_a = ar.get("first_name_norm", "")
            ln_a = ar.get("last_name_norm", "")
            if cn_detector.is_common(fn_a, ln_a):
                is_common_name_city = True

        # Try AUTO
        auto_result = _apply_auto_rules(sig, cfg, ar, br, cn_detector)
        if auto_result:
            rule, comment, rationale = auto_result
            base["rule"] = rule
            base["comments"] = comment
            base["rationale"] = rationale
            auto_rows.append(base)
            if rule.startswith("G3") and rule != "G3_NAME_SPL_CITY":
                cn_auto_promoted += 1
            continue

        # If common-name G3d fell through, track it and route to REVIEW
        # directly — bypass NOT-DUP rules (N1 license conflict should not
        # override a strong name+specialty+city match for common names;
        # the old G3 auto-merged these before N1 was ever checked).
        if is_common_name_city:
            cn_review_flagged += 1
            score, reasons, comment, rationale = _score_review(
                sig, ar, br, cfg, common_name_flag=True, cn_detector=cn_detector
            )
            if score >= review_threshold:
                base["score"] = score
                base["reasons"] = ",".join(reasons)
                base["comments"] = comment
                base["rationale"] = rationale
                review_rows.append(base)
            else:
                base["reason"] = "N3_LOW_SCORE"
                base["comments"] = f"Insufficient similarity (Score: {score})"
                base["rationale"] = (
                    f"Low similarity score ({score}) — not a candidate "
                    f"for duplicate review."
                )
                notdup_rows.append(base)
            continue

        # Try NOT-DUP (only for non-G3 common-name pairs)
        notdup_result = _apply_not_dup_rules(sig, ar, br, cfg)
        if notdup_result:
            reason, comment, rationale = notdup_result
            base["reason"] = reason
            base["comments"] = comment
            base["rationale"] = rationale
            notdup_rows.append(base)
            continue

        # Score for REVIEW
        score, reasons, comment, rationale = _score_review(
            sig, ar, br, cfg, common_name_flag=False, cn_detector=cn_detector
        )
        if score >= review_threshold:
            base["score"] = score
            base["reasons"] = ",".join(reasons)
            base["comments"] = comment
            base["rationale"] = rationale
            review_rows.append(base)
        else:
            base["reason"] = "N3_LOW_SCORE"
            base["comments"] = f"Insufficient similarity (Score: {score})"
            base["rationale"] = f"Low similarity score ({score}) — not a candidate for duplicate review."
            notdup_rows.append(base)

    # Step 5 — Cluster AUTO pairs
    _progress(87, f"Clustering {len(auto_rows):,} auto-merge pairs...")
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

    # ── UNIQUE VIDs (no candidate pairs at all) ──────────────────────
    all_canon_vids = set(canon["vid"])
    paired_vids: set[str] = set()
    for frame in (auto_df, review_df, notdup_df):
        if not frame.empty and "vid_a" in frame.columns:
            paired_vids |= set(frame["vid_a"].astype(str))
            paired_vids |= set(frame["vid_b"].astype(str))
    unique_vids = all_canon_vids - paired_vids

    unique_rows_list = []
    for vid in sorted(unique_vids):
        row = {"vid": vid, "status": "UNIQUE",
               "comments": "No candidate pairs found — confirmed unique"}
        if enrich and vid in canon_idx.index:
            r = canon_idx.loc[vid]
            row["name"] = r["display_name"]
            row["specialties"] = r.get("display_specialties", "")
            row["cities"] = r.get("display_cities", "")
        unique_rows_list.append(row)
    unique_df = pd.DataFrame(unique_rows_list) if unique_rows_list else pd.DataFrame()

    # Summary — including Effort Avoidance metrics (VID-based, not pair-based)
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
        "raw_rows": len(df),
        "unique_hcp_vids": len(canon),
        "candidate_pairs_evaluated": len(pairs),
        "auto_merge_pairs": len(auto_df),
        "auto_merge_vids": auto_merge_vids,
        "review_pairs": len(review_df),
        "review_vids": review_vids,
        "not_dup_pairs": len(notdup_df),
        "not_dup_vids": notdup_vids,
        "unique_vids": len(unique_vids),
        "auto_clusters": cluster_df["cluster_id"].nunique() if not cluster_df.empty else 0,
        "shared_phones": len(shared_det._shared_phones),
        "shared_emails": len(shared_det._shared_emails),
        "vids_auto_resolved": auto_merge_vids,
        "manual_velocity_per_hr": velocity,
        "effort_avoidance_hours": hours_saved,
        "est_remaining_review_hours": remaining_hours,
        "common_name_auto_promoted": cn_auto_promoted,
        "common_name_review_flagged": cn_review_flagged,
    }

    logger.info(
        "HCP Pipeline complete: %d AUTO, %d REVIEW, %d NOT_DUP, %d UNIQUE, %d clusters",
        summary["auto_merge_pairs"],
        summary["review_pairs"],
        summary["not_dup_pairs"],
        summary["unique_vids"],
        summary["auto_clusters"],
    )

    return {
        "hcp_canonical": canon,
        "hcp_auto": auto_df,
        "hcp_review": review_df,
        "hcp_notdup": notdup_df,
        "hcp_unique": unique_df,
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

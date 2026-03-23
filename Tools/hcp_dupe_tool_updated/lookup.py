"""
lookup.py — Single-record lookup and PDR batch pre-screening engine.

Modes:
  1. Single-record lookup: DS enters one HCP's details → check against
     pre-loaded reference DB → return ranked matches.
  2. PDR pre-screening: Upload a batch of new PDR records → match each
     against existing DB → flag dupes before processing.

Both modes reuse the existing BlockingEngine, canonicalization, and
scoring infrastructure from hcp_pipeline.py.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

try:
    from .core import (
        BlockingEngine,
        NameSimilarityCache,
        SharedContactDetector,
        SpecialtySynonymResolver,
        name_similarity,
        norm_email,
        norm_license,
        norm_phone,
        norm_text,
        strip_name_suffixes,
    )
    from .hcp_pipeline import (
        _build_canonical,
        _compute_signals,
        _apply_auto_rules,
        _score_review,
        _glob_match,
    )
    from .common_names import IndianCommonNameDetector
except ImportError:
    from core import (
        BlockingEngine,
        NameSimilarityCache,
        SharedContactDetector,
        SpecialtySynonymResolver,
        name_similarity,
        norm_email,
        norm_license,
        norm_phone,
        norm_text,
        strip_name_suffixes,
    )
    from hcp_pipeline import (
        _build_canonical,
        _compute_signals,
        _apply_auto_rules,
        _score_review,
        _glob_match,
    )
    from common_names import IndianCommonNameDetector

logger = logging.getLogger("dupe_tool.lookup")


# ── Reference DB Index ────────────────────────────────────────────────

class ReferenceIndex:
    """Pre-built index over an existing VOD extract for fast lookups.

    Build once, probe many times. Stored in st.session_state for reuse.
    """

    def __init__(
        self,
        canon_dict: dict[str, dict],
        blocker: BlockingEngine,
        shared_det: SharedContactDetector,
        cn_detector: IndianCommonNameDetector | None,
        spec_resolver: SpecialtySynonymResolver | None,
        cfg: dict[str, Any],
        vid_count: int,
    ):
        self.canon_dict = canon_dict
        self.blocker = blocker
        self.shared_det = shared_det
        self.cn_detector = cn_detector
        self.spec_resolver = spec_resolver
        self.cfg = cfg
        self.vid_count = vid_count
        self._nsim_cache = NameSimilarityCache(max_size=50_000)


def build_reference_index(
    df: pd.DataFrame,
    cfg: dict[str, Any],
    progress_fn=None,
) -> ReferenceIndex:
    """Build a reference index from a full VOD extract.

    This is the expensive step — run once, then reuse for all lookups.
    """

    def _progress(msg):
        if progress_fn:
            progress_fn(msg)
        logger.info(msg)

    _progress("Building reference index: canonicalizing records...")

    canon = _build_canonical(df, cfg, progress_fn=_progress)

    _progress(f"Reference index: {len(canon):,} unique VIDs canonicalized")

    # Build dict index
    canon_dict: dict[str, dict] = {}
    for _, row in canon.iterrows():
        canon_dict[row["vid"]] = row.to_dict()

    # Shared contacts
    _progress("Reference index: detecting shared contacts...")
    shared_det = SharedContactDetector(
        threshold=cfg.get("shared_contact", {}).get("threshold", 5)
    )
    for rec in canon_dict.values():
        shared_det.feed(set(rec["phones"]), set(rec["emails"]))
    shared_det.finalize()

    # Common name detector
    cn_cfg = cfg.get("common_name_detection", {})
    cn_detector = None
    if cn_cfg.get("enabled", True):
        cn_detector = IndianCommonNameDetector(cn_cfg)

    # Specialty synonym resolver
    spec_resolver = SpecialtySynonymResolver(cfg.get("specialty_synonyms"))

    # Build blocking index
    _progress("Reference index: building blocking keys...")
    blk_cfg = cfg.get("blocking", {})
    blocker = BlockingEngine(
        max_block_size=blk_cfg.get("max_block_size", 500),
        phonetic=blk_cfg.get("phonetic_blocking", True),
        first_initial=blk_cfg.get("first_initial_blocking", True),
        spec_resolver=spec_resolver,
    )
    for rec in canon_dict.values():
        blocker.add_hcp(rec["vid"], rec)

    _progress("Reference index ready.")

    return ReferenceIndex(
        canon_dict=canon_dict,
        blocker=blocker,
        shared_det=shared_det,
        cn_detector=cn_detector,
        spec_resolver=spec_resolver,
        cfg=cfg,
        vid_count=len(canon),
    )


# ── Single-Record Lookup ──────────────────────────────────────────────

def _form_to_canonical(form: dict[str, str], cfg: dict[str, Any]) -> dict:
    """Convert a form input dict into a canonical record for probing.

    Form keys: first_name, last_name, specialty, city, state, postal,
               phone, email, license_number, license_body.
    """
    suffixes = cfg.get("name_matching", {}).get("strip_suffixes", [])

    first_norm = norm_text(form.get("first_name", ""))
    last_norm = norm_text(form.get("last_name", ""))
    parts = [p for p in (first_norm, last_norm) if p]
    name_canon = " ".join(parts).strip()
    if suffixes:
        name_canon = strip_name_suffixes(name_canon, suffixes)

    phone = norm_phone(form.get("phone", ""))
    email = norm_email(form.get("email", ""))
    lic_num = norm_license(form.get("license_number", ""))
    lic_body = norm_text(form.get("license_body", ""))
    spec = norm_text(form.get("specialty", ""))
    city = norm_text(form.get("city", ""))
    state = norm_text(form.get("state", ""))
    postal = "".join(ch for ch in str(form.get("postal", "")) if ch.isdigit())

    phones = [phone] if phone else []
    emails = [email] if email else []
    specialties = [spec] if spec else []
    active_licenses = [(lic_num, lic_body)] if lic_num and lic_body else []
    pins = [postal] if postal else []

    return {
        "vid": "__PROBE__",
        "name_canon": name_canon,
        "first_name_norm": first_norm,
        "last_name_norm": last_norm,
        "city_cda_norm": city,
        "specialties": specialties,
        "phones": phones,
        "emails": emails,
        "licenses": list(active_licenses),
        "active_licenses": active_licenses,
        "hco_vids": [],
        "pins": pins,
        "cities": [city] if city else [],
        "states": [state] if state else [],
        "hcp_status": "",
        "candidate_record": "",
        "display_name": f"{form.get('first_name', '')} {form.get('last_name', '')}".strip(),
        "display_specialties": form.get("specialty", ""),
        "display_cities": form.get("city", ""),
        "display_phones": form.get("phone", ""),
        "display_emails": form.get("email", ""),
    }


def lookup_single(
    form: dict[str, str],
    ref: ReferenceIndex,
) -> list[dict[str, Any]]:
    """Look up a single record against the reference index.

    Returns a list of match dicts sorted by confidence (best first).
    Each dict has: vid, verdict, score, rule, name_similarity, signals,
    and enrichment fields from the matched DB record.
    """
    cfg = ref.cfg
    probe = _form_to_canonical(form, cfg)

    # Probe the blocking index for candidate VIDs
    candidates = ref.blocker.probe_hcp(probe)
    if not candidates:
        return []

    results: list[dict[str, Any]] = []

    for vid in candidates:
        db_rec = ref.canon_dict[vid]
        sig = _compute_signals(
            probe, db_rec, ref.shared_det, name_cache=ref._nsim_cache,
            spec_resolver=ref.spec_resolver
        )

        # Skip very low name similarity (noise)
        if sig["nscore"] < 60:
            continue

        # Try AUTO rules
        auto_result = _apply_auto_rules(sig, cfg, probe, db_rec, ref.cn_detector)
        if auto_result:
            rule, comment, rationale = auto_result
            results.append({
                "vid": vid,
                "verdict": "LIKELY_DUPLICATE",
                "confidence": "HIGH",
                "score": 100,
                "rule": rule,
                "name_similarity": round(sig["nscore"], 1),
                "comment": comment,
                "rationale": rationale,
                "name": db_rec.get("display_name", ""),
                "specialties": db_rec.get("display_specialties", ""),
                "cities": db_rec.get("display_cities", ""),
                "phones": db_rec.get("display_phones", ""),
                "emails": db_rec.get("display_emails", ""),
                "matched_phones": ";".join(sorted(sig["phones_overlap"])),
                "matched_emails": ";".join(sorted(sig["emails_overlap"])),
                "license_info": sig["license_info"],
            })
            continue

        # Score as review
        score, reasons, comment, rationale = _score_review(
            sig, probe, db_rec, cfg, common_name_flag=False,
            cn_detector=ref.cn_detector
        )

        review_threshold = cfg.get("hcp_review_scoring", {}).get("review_threshold", 50)

        if score >= review_threshold:
            confidence = "MEDIUM" if score >= 70 else "LOW"
            results.append({
                "vid": vid,
                "verdict": "POSSIBLE_MATCH",
                "confidence": confidence,
                "score": score,
                "rule": "",
                "name_similarity": round(sig["nscore"], 1),
                "comment": comment,
                "rationale": rationale,
                "name": db_rec.get("display_name", ""),
                "specialties": db_rec.get("display_specialties", ""),
                "cities": db_rec.get("display_cities", ""),
                "phones": db_rec.get("display_phones", ""),
                "emails": db_rec.get("display_emails", ""),
                "matched_phones": ";".join(sorted(sig["phones_overlap"])),
                "matched_emails": ";".join(sorted(sig["emails_overlap"])),
                "license_info": sig["license_info"],
            })

    # Sort by score descending
    results.sort(key=lambda r: (-r["score"], -r["name_similarity"]))
    return results


# ── PDR Batch Pre-Screening ───────────────────────────────────────────

def screen_pdr_batch(
    pdr_df: pd.DataFrame,
    ref: ReferenceIndex,
    progress_fn=None,
) -> pd.DataFrame:
    """Screen a batch of new PDR records against the reference DB.

    Args:
        pdr_df: DataFrame of new PDR records (same Veeva column format).
        ref: Pre-built ReferenceIndex from existing DB.
        progress_fn: Optional callable(pct, msg) for progress updates.

    Returns:
        DataFrame with one row per PDR record, annotated with:
        - pdr_verdict: LIKELY_DUP / POSSIBLE_MATCH / CLEAN
        - best_match_vid, best_match_score, best_match_rule
        - best_match_name, best_match_rationale
        - match_count (total matches found)
    """
    cfg = ref.cfg

    def _progress(pct, msg):
        if progress_fn:
            progress_fn(pct, msg)
        logger.info(msg)

    _progress(5, "PDR Pre-Screen: canonicalizing PDR records...")

    # Canonicalize PDR records using the same pipeline
    pdr_canon = _build_canonical(pdr_df, cfg, progress_fn=lambda m: _progress(10, m))

    if pdr_canon.empty:
        return pd.DataFrame()

    # Build dict for PDR records
    pdr_dict: dict[str, dict] = {}
    for _, row in pdr_canon.iterrows():
        pdr_dict[row["vid"]] = row.to_dict()

    _progress(20, f"PDR Pre-Screen: checking {len(pdr_dict):,} PDR records against {ref.vid_count:,} DB records...")

    total = len(pdr_dict)
    progress_interval = max(1, total // 20)
    output_rows: list[dict] = []

    for idx, (pdr_vid, pdr_rec) in enumerate(pdr_dict.items()):
        if idx % progress_interval == 0:
            pct = 20 + int(70 * idx / total)
            _progress(pct, f"Screening PDR records... {idx:,}/{total:,}")

        # Probe blocking index
        candidates = ref.blocker.probe_hcp(pdr_rec)

        best_verdict = "CLEAN"
        best_score = 0
        best_rule = ""
        best_vid = ""
        best_name = ""
        best_rationale = ""
        best_nsim = 0.0
        match_count = 0

        for db_vid in candidates:
            db_rec = ref.canon_dict[db_vid]
            sig = _compute_signals(
                pdr_rec, db_rec, ref.shared_det, name_cache=ref._nsim_cache,
                spec_resolver=ref.spec_resolver
            )

            if sig["nscore"] < 60:
                continue

            # Try AUTO rules
            auto_result = _apply_auto_rules(sig, cfg, pdr_rec, db_rec, ref.cn_detector)
            if auto_result:
                rule, comment, rationale = auto_result
                match_count += 1
                if best_verdict != "LIKELY_DUP" or sig["nscore"] > best_nsim:
                    best_verdict = "LIKELY_DUP"
                    best_score = 100
                    best_rule = rule
                    best_vid = db_vid
                    best_name = db_rec.get("display_name", "")
                    best_rationale = rationale
                    best_nsim = sig["nscore"]
                continue

            # Score for review
            score, reasons, comment, rationale = _score_review(
                sig, pdr_rec, db_rec, cfg, common_name_flag=False,
                cn_detector=ref.cn_detector
            )
            review_threshold = cfg.get("hcp_review_scoring", {}).get("review_threshold", 50)

            if score >= review_threshold:
                match_count += 1
                if best_verdict == "CLEAN" or (best_verdict == "POSSIBLE_MATCH" and score > best_score):
                    best_verdict = "POSSIBLE_MATCH"
                    best_score = score
                    best_rule = ""
                    best_vid = db_vid
                    best_name = db_rec.get("display_name", "")
                    best_rationale = rationale
                    best_nsim = sig["nscore"]

        output_rows.append({
            "pdr_vid": pdr_vid,
            "pdr_name": pdr_rec.get("display_name", ""),
            "pdr_specialties": pdr_rec.get("display_specialties", ""),
            "pdr_cities": pdr_rec.get("display_cities", ""),
            "pdr_phones": pdr_rec.get("display_phones", ""),
            "pdr_emails": pdr_rec.get("display_emails", ""),
            "pdr_verdict": best_verdict,
            "match_count": match_count,
            "best_match_vid": best_vid,
            "best_match_score": best_score,
            "best_match_rule": best_rule,
            "best_match_name_sim": round(best_nsim, 1),
            "best_match_name": best_name,
            "best_match_rationale": best_rationale,
        })

    _progress(95, "PDR Pre-Screen: finalizing results...")

    result_df = pd.DataFrame(output_rows)

    # Sort: LIKELY_DUP first, then POSSIBLE_MATCH, then CLEAN
    verdict_order = {"LIKELY_DUP": 0, "POSSIBLE_MATCH": 1, "CLEAN": 2}
    if not result_df.empty:
        result_df["_sort"] = result_df["pdr_verdict"].map(verdict_order)
        result_df = result_df.sort_values(
            ["_sort", "best_match_score"], ascending=[True, False]
        ).drop(columns=["_sort"]).reset_index(drop=True)

    _progress(100, "PDR Pre-Screen complete.")
    return result_df

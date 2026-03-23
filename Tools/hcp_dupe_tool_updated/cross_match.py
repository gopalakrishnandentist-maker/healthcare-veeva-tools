"""
cross_match.py — Cross-DB matching engine for external HCP source files.

Matches records from any HCP source file (Veeva Link KOL exports, custom
lists, etc.) against a pre-built master Veeva OpenData reference index.

Features:
    - Smart header-row detection (handles Veeva Link files with offset headers)
    - Column auto-detection (maps arbitrary column names to HCP data points)
    - Per-record canonical adapter (converts any source row to pipeline format)
    - Batch cross-match with full AUTO + REVIEW scoring

Usage:
    from cross_match import detect_header_row, auto_detect_columns, cross_match_batch
"""

from __future__ import annotations

import io
import logging
import re
from typing import Any

import pandas as pd

from core import (
    norm_email,
    norm_phone,
    norm_text,
    strip_name_suffixes,
)
from hcp_pipeline import (
    _apply_auto_rules,
    _compute_signals,
    _score_review,
)
from lookup import ReferenceIndex

logger = logging.getLogger("dupe_tool.cross_match")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1.  Smart Header-Row Detection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def detect_header_row(
    file_bytes: bytes,
    filename: str,
    sheet_name: str | None = None,
) -> tuple[pd.DataFrame, int]:
    """Read an Excel/CSV file, auto-detect the header row, return clean DF.

    Strategy:
        1. Read first 15 rows raw (header=None).
        2. Score each row: count cells that look like column headers
           (short alphanumeric strings, contain letters, not purely numeric).
        3. Row with highest score wins.
        4. Re-read using that row as header.

    Returns:
        (DataFrame with correct headers, 0-based header row index)
    """
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    buf = io.BytesIO(file_bytes)

    # ── Determine sheet ─────────────────────────────────
    target_sheet = None
    if ext in ("xlsx", "xls"):
        xl = pd.ExcelFile(buf)
        if sheet_name and sheet_name in xl.sheet_names:
            target_sheet = sheet_name
        elif "Your List" in xl.sheet_names:
            target_sheet = "Your List"
        else:
            target_sheet = xl.sheet_names[0]
        buf.seek(0)

    # ── Read preview rows ───────────────────────────────
    if ext in ("xlsx", "xls"):
        preview = pd.read_excel(
            buf, sheet_name=target_sheet, header=None, nrows=15, dtype=str,
        )
    else:
        buf.seek(0)
        preview = pd.read_csv(buf, header=None, nrows=15, dtype=str)

    # ── Score each row ──────────────────────────────────
    _HEADER_RE = re.compile(r"[a-zA-Z]")

    best_row = 0
    best_score = 0
    for row_idx in range(min(15, len(preview))):
        score = 0
        for cell in preview.iloc[row_idx]:
            if pd.isna(cell):
                continue
            s = str(cell).strip()
            if (
                1 < len(s) < 80
                and _HEADER_RE.search(s)
                and not s.replace(".", "").replace(" ", "").isdigit()
            ):
                score += 1
        if score > best_score:
            best_score = score
            best_row = row_idx

    # ── Re-read with detected header ────────────────────
    buf.seek(0)
    if ext in ("xlsx", "xls"):
        df = pd.read_excel(
            buf, sheet_name=target_sheet, header=best_row, dtype=str,
        )
    else:
        df = pd.read_csv(buf, header=best_row, dtype=str)

    # Drop rows/columns that are entirely NaN
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # Drop unnamed columns (artefacts from merged cells)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    logger.info(
        "Header detected at row %d  |  %d data rows  |  %d columns",
        best_row, len(df), len(df.columns),
    )
    return df, best_row


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2.  Column Auto-Detection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Rules: logical_field → list of (pattern, is_regex, priority)
# Higher priority wins. Exact matches are always preferred.
_COLUMN_RULES: dict[str, list[tuple[str, bool, int]]] = {
    "vid": [
        ("opendatavid", False, 10),
        ("veevaid", False, 8),
        (r"hcp\.vid__v", True, 9),
        (r"network.?id", True, 7),
    ],
    "link_id": [
        ("link-id", False, 10),
        ("linkid", False, 9),
    ],
    "first_name": [
        ("first name", False, 10),
        (r"first.?name", True, 8),
    ],
    "middle_name": [
        ("middle name", False, 10),
        (r"middle.?name", True, 8),
    ],
    "last_name": [
        ("last name", False, 10),
        (r"last.?name", True, 8),
        (r"surname", True, 7),
    ],
    "full_name": [
        ("original full name", False, 10),
        (r"full.?name", True, 8),
    ],
    "specialty": [
        ("specialties", False, 10),
        ("specialty", False, 9),
        (r"specialt", True, 7),
    ],
    "email": [
        ("emails", False, 10),
        ("email", False, 9),
        (r"e.?mail", True, 7),
    ],
    "phone": [
        ("phone", False, 10),
        ("mobile", False, 8),
        (r"phone|mobile|tel", True, 6),
    ],
    "city": [
        ("city", False, 10),
        (r"city|town|locality", True, 7),
    ],
    "state": [
        ("state", False, 10),
        (r"state.province|state/province", True, 9),
        (r"address\.administrative_area__v", True, 9),
        (r"(?<!record_)(?<!sub_administrative_)(?:state|province|region)", True, 7),
    ],
    "postal": [
        ("postal code", False, 10),
        ("zip code", False, 9),
        ("zip", False, 8),
        ("pincode", False, 8),
        (r"postal|zip|pincode|pin.?code", True, 6),
    ],
    "country": [
        ("country", False, 10),
        (r"primary_country|country__v", True, 8),
    ],
    "affiliation": [
        ("affiliations", False, 10),
        ("affiliation", False, 9),
        (r"affiliat|hospital|institution|organization", True, 7),
        (r"parent_hco_name|hco.*name", True, 5),
    ],
    "credentials": [
        ("professional credentials", False, 10),
        ("credentials", False, 9),
        (r"credential|degree|qualification", True, 7),
    ],
}

# Columns that should be collected as multi-column lists (not single-column).
# These are matched by regex against lowered column names.
_MULTI_COLUMN_RULES: dict[str, list[tuple[str, int]]] = {
    # HCO VID columns — collect parent + grandparent HCO VIDs
    "hco_vids": [
        (r"hco\.(?:parent_)?hco_vid|hco_vid__v|parent_hco_vid|grandparent_hco_vid", 1),
    ],
    # Veeva-style multi-columns for specialties, phones, emails
    "specialties_multi": [
        (r"hcp\.specialty_\d+__v|specialty_\d+", 1),
    ],
    "phones_multi": [
        (r"hcp\.phone_\d+__v", 1),
    ],
    "emails_multi": [
        (r"hcp\.email_\d+__v", 1),
    ],
}


def auto_detect_columns(df: pd.DataFrame) -> dict[str, str | None | list[str]]:
    """Auto-detect which DataFrame columns map to which logical HCP fields.

    Returns
    -------
    dict
        Mapping of logical field name → actual column name (or None).
        Multi-column fields (like hco_vids) map to a list of column names.
        Example: {"first_name": "First Name", "email": "Emails", "phone": None,
                  "hco_vids": ["hco.parent_hco_vid__v (...)", "hco.grandparent_hco_vid__v (...)"]}
    """
    detected: dict[str, str | None | list[str]] = {}
    used_columns: set[str] = set()

    for logical_field, rules in _COLUMN_RULES.items():
        best_col: str | None = None
        best_priority = 0

        for df_col in df.columns:
            if df_col in used_columns:
                continue
            col_lower = df_col.strip().lower()

            for pattern, is_regex, priority in rules:
                if priority <= best_priority:
                    continue
                if is_regex:
                    if re.search(pattern, col_lower):
                        best_col = df_col
                        best_priority = priority
                else:
                    if col_lower == pattern:
                        best_col = df_col
                        best_priority = priority

        detected[logical_field] = best_col
        if best_col:
            used_columns.add(best_col)

    # Multi-column detection (e.g. HCO VIDs from Veeva-format files)
    for logical_field, patterns in _MULTI_COLUMN_RULES.items():
        matched_cols: list[str] = []
        for df_col in df.columns:
            col_lower = df_col.strip().lower()
            for pattern, _ in patterns:
                if re.search(pattern, col_lower):
                    matched_cols.append(df_col)
                    break
        if matched_cols:
            detected[logical_field] = matched_cols
            logger.info("Multi-column detection: %s → %s", logical_field, matched_cols)

    logger.info("Column auto-detection: %s", detected)
    return detected


def get_missing_data_warnings(col_map: dict[str, str | None]) -> list[str]:
    """Return user-facing warnings about missing data points."""
    warnings: list[str] = []
    if not col_map.get("phone"):
        warnings.append(
            "No phone column detected — AUTO rules G3a, G5 cannot fire. "
            "Matching relies on name + specialty + city + email + postal."
        )
    has_hco = bool(col_map.get("affiliation")) or bool(col_map.get("hco_vids"))
    if not has_hco:
        warnings.append(
            "No HCO/affiliation VID column — AUTO rule G1 cannot fire."
        )
    return warnings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3.  Source Row → Canonical Record Adapter
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_MULTI_SEP = re.compile(r"[;,|]")


def _source_row_to_canonical(
    row: pd.Series,
    col_map: dict[str, str | None],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Convert one source-file row into a canonical record dict.

    Produces the exact same key-set that ``_compute_signals()``,
    ``_apply_auto_rules()`` and ``_score_review()`` consume.
    """
    suffixes = cfg.get("name_matching", {}).get("strip_suffixes", [])

    def _get(field: str) -> str:
        col = col_map.get(field)
        if not col:
            return ""
        val = row.get(col)
        if pd.isna(val):
            return ""
        return str(val).strip()

    # ── VID selection ─────────────────────────────────
    opendata_vid = _get("vid")
    link_id = _get("link_id")
    if opendata_vid:
        vid = opendata_vid
    elif link_id:
        vid = f"LINK_{link_id}"
    else:
        vid = f"ROW_{row.name}"

    # ── Names ─────────────────────────────────────────
    first_raw = _get("first_name")
    middle_raw = _get("middle_name")
    last_raw = _get("last_name")
    first_norm = norm_text(first_raw)
    mid_norm = norm_text(middle_raw)
    last_norm = norm_text(last_raw)

    parts = [p for p in (first_norm, mid_norm, last_norm) if p]
    name_canon = " ".join(parts).strip()
    if suffixes:
        name_canon = strip_name_suffixes(name_canon, suffixes)

    # Fall back to full_name if first/last empty
    if not name_canon:
        full_raw = _get("full_name")
        if full_raw:
            name_canon = norm_text(full_raw)

    # ── Emails (may be semicolon-separated or multi-column) ───────────
    emails: list[str] = []
    email_multi_cols = col_map.get("emails_multi")
    if isinstance(email_multi_cols, list) and email_multi_cols:
        for ec in email_multi_cols:
            val = row.get(ec)
            if pd.notna(val):
                ne = norm_email(str(val).strip())
                if ne:
                    emails.append(ne)
        raw_email = "; ".join(str(row.get(c, "")) for c in email_multi_cols if pd.notna(row.get(c)))
    else:
        raw_email = _get("email")
        if raw_email:
            for e in _MULTI_SEP.split(raw_email):
                ne = norm_email(e.strip())
                if ne:
                    emails.append(ne)

    # ── Phones (may be single or multi-column) ──────────
    phones: list[str] = []
    phone_multi_cols = col_map.get("phones_multi")
    if isinstance(phone_multi_cols, list) and phone_multi_cols:
        for pc in phone_multi_cols:
            val = row.get(pc)
            if pd.notna(val):
                np_ = norm_phone(str(val).strip())
                if np_:
                    phones.append(np_)
        raw_phone = "; ".join(str(row.get(c, "")) for c in phone_multi_cols if pd.notna(row.get(c)))
    else:
        raw_phone = _get("phone")
        if raw_phone:
            for p in _MULTI_SEP.split(raw_phone):
                np_ = norm_phone(p.strip())
                if np_:
                    phones.append(np_)

    # ── Specialties (may be semicolon-separated or multi-column) ──────
    specialties: list[str] = []
    spec_multi_cols = col_map.get("specialties_multi")
    if isinstance(spec_multi_cols, list) and spec_multi_cols:
        for sc in spec_multi_cols:
            val = row.get(sc)
            if pd.notna(val):
                ns = norm_text(str(val).strip())
                if ns:
                    specialties.append(ns)
        raw_spec = "; ".join(str(row.get(c, "")) for c in spec_multi_cols if pd.notna(row.get(c)))
    else:
        raw_spec = _get("specialty")
        if raw_spec:
            for s in _MULTI_SEP.split(raw_spec):
                ns = norm_text(s.strip())
                if ns:
                    specialties.append(ns)

    # ── Geo ───────────────────────────────────────────
    city = norm_text(_get("city"))
    state = norm_text(_get("state"))
    postal_raw = _get("postal")
    postal = "".join(ch for ch in postal_raw if ch.isdigit()) if postal_raw else ""

    cities = [city] if city else []
    states = [state] if state else []
    pins = [postal] if postal else []

    # ── HCO VIDs (from Veeva-style multi-columns) ──────
    hco_vids: list[str] = []
    hco_vid_cols = col_map.get("hco_vids")
    if isinstance(hco_vid_cols, list):
        for hvc in hco_vid_cols:
            val = row.get(hvc)
            if pd.notna(val):
                nv = norm_text(str(val).strip())
                if nv:
                    hco_vids.append(nv)

    return {
        # Core canonical fields (must match _compute_signals keys)
        "vid": vid,
        "name_canon": name_canon,
        "first_name_norm": first_norm if first_norm else (norm_text(_get("full_name").split()[0]) if _get("full_name") else ""),
        "last_name_norm": last_norm if last_norm else (norm_text(_get("full_name").split()[-1]) if _get("full_name") else ""),
        "city_cda_norm": city,
        "specialties": specialties,
        "phones": phones,
        "emails": emails,
        "licenses": [],
        "active_licenses": [],
        "hco_vids": hco_vids,
        "pins": pins,
        "cities": cities,
        "states": states,
        "hcp_status": "",
        "candidate_record": "",
        # Display enrichment (raw, unnormalized)
        "display_name": f"{first_raw} {middle_raw} {last_raw}".replace("  ", " ").strip(),
        "display_specialties": raw_spec,
        "display_cities": _get("city"),
        "display_phones": raw_phone,
        "display_emails": raw_email,
        # Source metadata (preserved for output)
        "_source_link_id": link_id,
        "_source_affiliation": _get("affiliation"),
        "_source_credentials": _get("credentials"),
        "_source_country": _get("country"),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4.  Batch Cross-Match Pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def cross_match_batch(
    source_df: pd.DataFrame,
    col_map: dict[str, str | None],
    ref: ReferenceIndex,
    progress_fn: Any = None,
) -> pd.DataFrame:
    """Match source records against master reference DB.

    For each source record:
        1. Convert to canonical format via adapter.
        2. Check for exact VID match in reference index.
        3. Probe blocking index for candidates.
        4. Score candidates (AUTO rules → REVIEW scoring).
        5. Classify as LIKELY_DUP / POSSIBLE_MATCH / CLEAN.

    Returns
    -------
    pd.DataFrame
        One row per source record with verdict, best match details, and
        source enrichment columns.
    """
    cfg = ref.cfg

    def _progress(pct: int, msg: str) -> None:
        if progress_fn:
            progress_fn(pct, msg)

    _progress(5, "Converting source records to canonical format...")

    # ── Convert all source rows ─────────────────────────
    source_records: list[dict[str, Any]] = []
    for _, row in source_df.iterrows():
        canon = _source_row_to_canonical(row, col_map, cfg)
        if not canon["name_canon"]:
            continue  # skip rows with no usable name
        source_records.append(canon)

    if not source_records:
        _progress(100, "No valid source records found.")
        return pd.DataFrame()

    total = len(source_records)
    _progress(
        10,
        f"Screening {total:,} source records against "
        f"{ref.vid_count:,} master DB records...",
    )

    review_threshold = cfg.get(
        "hcp_review_scoring", {},
    ).get("review_threshold", 50)

    progress_interval = max(1, total // 20)
    output_rows: list[dict[str, Any]] = []

    for idx, src_rec in enumerate(source_records):
        if idx % progress_interval == 0:
            pct = 10 + int(80 * idx / total)
            _progress(pct, f"Screening source records… {idx:,}/{total:,}")

        best_verdict = "CLEAN"
        best_score = 0
        best_rule = ""
        best_vid = ""
        best_name = ""
        best_rationale = ""
        best_nsim = 0.0
        best_match_rec: dict[str, Any] | None = None
        match_count = 0

        # VID is reference-only — never used as a matching signal.
        # Matching is always based on data content (name, specialty,
        # city, email, etc.) via AUTO rules + REVIEW scoring.

        # ── Probe blocking index ────────────────────────
        candidates = ref.blocker.probe_hcp(src_rec)

        for db_vid in candidates:
            db_rec = ref.canon_dict[db_vid]
            sig = _compute_signals(
                src_rec, db_rec, ref.shared_det, name_cache=ref._nsim_cache,
                spec_resolver=ref.spec_resolver,
            )

            if sig["nscore"] < 60:
                continue

            # ── Try AUTO rules ──────────────────────────
            auto_result = _apply_auto_rules(
                sig, cfg, src_rec, db_rec, ref.cn_detector,
            )
            if auto_result:
                rule, _comment, rationale = auto_result
                match_count += 1
                if best_verdict != "LIKELY_DUP" or sig["nscore"] > best_nsim:
                    best_verdict = "LIKELY_DUP"
                    best_score = 100
                    best_rule = rule
                    best_vid = db_vid
                    best_name = db_rec.get("display_name", "")
                    best_rationale = rationale
                    best_nsim = sig["nscore"]
                    best_match_rec = db_rec
                continue

            # ── Score for REVIEW ────────────────────────
            score, _reasons, _comment, rationale = _score_review(
                sig, src_rec, db_rec, cfg, common_name_flag=False,
                cn_detector=ref.cn_detector,
            )
            if score >= review_threshold:
                match_count += 1
                if best_verdict == "CLEAN" or (
                    best_verdict == "POSSIBLE_MATCH" and score > best_score
                ):
                    best_verdict = "POSSIBLE_MATCH"
                    best_score = score
                    best_rule = ""
                    best_vid = db_vid
                    best_name = db_rec.get("display_name", "")
                    best_rationale = rationale
                    best_nsim = sig["nscore"]
                    best_match_rec = db_rec

        # ── Build output row ────────────────────────────
        output_rows.append({
            # Source record
            "source_vid": src_rec["vid"],
            "source_link_id": src_rec.get("_source_link_id", ""),
            "source_name": src_rec.get("display_name", ""),
            "source_specialties": src_rec.get("display_specialties", ""),
            "source_city": src_rec.get("display_cities", ""),
            "source_email": src_rec.get("display_emails", ""),
            "source_affiliation": src_rec.get("_source_affiliation", ""),
            "source_country": src_rec.get("_source_country", ""),
            # Verdict
            "xmatch_verdict": best_verdict,
            "match_count": match_count,
            # Best match
            "best_match_vid": best_vid,
            "best_match_score": best_score,
            "best_match_rule": best_rule,
            "best_match_name_sim": round(best_nsim, 1),
            "best_match_name": best_name,
            "best_match_specialties": (
                best_match_rec.get("display_specialties", "")
                if best_match_rec else ""
            ),
            "best_match_cities": (
                best_match_rec.get("display_cities", "")
                if best_match_rec else ""
            ),
            "best_match_emails": (
                best_match_rec.get("display_emails", "")
                if best_match_rec else ""
            ),
            "best_match_phones": (
                best_match_rec.get("display_phones", "")
                if best_match_rec else ""
            ),
            "best_match_rationale": best_rationale,
        })

    _progress(95, "Finalizing cross-match results...")

    result_df = pd.DataFrame(output_rows)

    # Sort: LIKELY_DUP → POSSIBLE_MATCH → CLEAN, then by score desc
    if not result_df.empty:
        verdict_order = {"LIKELY_DUP": 0, "POSSIBLE_MATCH": 1, "CLEAN": 2}
        result_df["_sort"] = result_df["xmatch_verdict"].map(verdict_order)
        result_df = (
            result_df.sort_values(
                ["_sort", "best_match_score"],
                ascending=[True, False],
            )
            .drop(columns=["_sort"])
            .reset_index(drop=True)
        )

    _progress(100, f"Cross-match complete. {total:,} records screened.")
    return result_df

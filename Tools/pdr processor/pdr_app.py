"""
PDR Processing Dashboard — Streamlit App
==========================================
AI-Augmented PDR triage tool with interactive dashboard.

Setup (one-time):
    pip install streamlit pandas openpyxl plotly rapidfuzz

Run:
    streamlit run pdr_app.py

This app runs 100% locally. No data is sent to any cloud service.
"""

import streamlit as st
import pandas as pd
import re
import io
import time
from datetime import datetime
from collections import defaultdict
from difflib import SequenceMatcher

# Try to import rapidfuzz for better performance; fall back to difflib
try:
    from rapidfuzz import fuzz as rf_fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    USE_RAPIDFUZZ = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    USE_PLOTLY = True
except ImportError:
    USE_PLOTLY = False


# =============================================================================
# PAGE CONFIG
# =============================================================================

# ── GK.Ai shared theme ──────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from gkai_theme import inject_gkai_theme, GKAI_PAGE_CONFIG, render_app_header, render_sidebar_nav

st.set_page_config(
    **GKAI_PAGE_CONFIG,
    page_title="PDR Processing Dashboard",
    page_icon="🏥",
)
inject_gkai_theme()


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================

class Config:
    """Tunable parameters — exposed via sidebar sliders."""

    # Defaults
    EXACT_MATCH_THRESHOLD = 92
    PARTIAL_MATCH_THRESHOLD = 70

    OUT_OF_SCOPE_SPECIALTIES = [
        "dentist", "dental", "bds", "mds", "orthodont",
        "pharmacy", "pharmacist", "pharm.d", "b.pharm", "m.pharm", "d.pharm",
        "homeopath", "bhms", "homoeopath",
        "ayurved", "bams", "ayush",
        "unani", "bums",
        "siddha", "bsms",
        "naturopath", "bnys",
        "veterinar", "bvsc", "b.v.sc",
        "physiotherap", "bpt", "mpt",
        "nursing", "b.sc nursing", "gnm",
        "optometr", "yoga",
    ]

    IN_SCOPE_DEGREES = [
        "mbbs", "md", "ms", "dm", "mch", "dnb", "diploma",
        "frcs", "mrcp", "frcp", "frcog",
    ]

    BUSINESS_TITLES = [
        "ceo", "coo", "cfo", "cmo", "cto",
        "director", "manager", "administrator",
        "president", "vice president", "vp",
        "chairman", "chairperson", "head of", "chief",
    ]

    JUNK_PATTERNS = [
        r"(?i)review[s]?\s+(by|from|on)",
        r"(?i)author[s]?\s*:",
        r"(?i)(rating|stars?|feedback)",
        r"(?i)(association|society|council|forum|federation)\b",
        r"\d{10,}",
    ]

    MIN_NAME_LENGTH = 2
    MAX_NAME_LENGTH = 60


# =============================================================================
# STEWARD NOTE PARSER
# =============================================================================

class NoteParser:
    @staticmethod
    def detect_format(note):
        if not note or not isinstance(note, str):
            return "unknown"
        if "// City:" in note or "// HCO_VID:" in note or "// HCP_Name:" in note:
            return "manual"
        if "** parenthco vid:" in note.lower() or "source url:" in note.lower():
            return "omnia"
        return "unknown"

    @staticmethod
    def parse_manual(note):
        result = {
            "city": None, "hco_vid": None, "hco_name": None, "hco_url": None,
            "hcp_name": None, "specialty": None, "qualifications": None,
            "scope_status": None, "source_for_listing": None, "department": None,
            "parse_format": "manual",
        }
        if not note or not isinstance(note, str):
            return result

        note_clean = note.strip().strip('"')
        patterns = {
            "city": r"//\s*City:\s*([^/]+?)(?://|Created|$)",
            "hco_vid": r"//\s*HCO_VID:\s*(\d+)",
            "hco_name": r"//\s*HCO_Name:\s*([^/]+?)(?://|Created|$)",
            "hco_url": r"//\s*HCO_URL:\s*(\S+?)(?=\s+//|\s+Created|\s*$)",
            "hcp_name": r"//\s*HCP_Name:\s*([^/]+?)(?://|Created|$)",
            "scope_status": r"//\s*Scope_status:\s*([^/]+?)(?://|Created|$)",
            "source_for_listing": r"//\s*source_for_hcp_listing:\s*([^/]+?)(?://|Created|$)",
        }
        for key, pattern in patterns.items():
            match = re.search(pattern, note_clean, re.IGNORECASE)
            if match:
                result[key] = match.group(1).strip()
        return result

    @staticmethod
    def parse_omnia(note):
        result = {
            "city": None, "hco_vid": None, "hco_name": None, "hco_url": None,
            "hcp_name": None, "specialty": None, "qualifications": None,
            "scope_status": None, "source_for_listing": None, "department": None,
            "first_name": None, "last_name": None, "middle_name": None,
            "salutation": None, "title": None, "parse_format": "omnia",
        }
        if not note or not isinstance(note, str):
            return result

        note_clean = note.strip().strip('"')

        url_match = re.search(r"source url:\s*(\S+?)(?:\s*\*\*|$)", note_clean, re.IGNORECASE)
        if url_match:
            result["hco_url"] = url_match.group(1).strip()

        patterns = {
            "hco_vid": r"\*\*\s*parenthco vid:\s*(\d+)",
            "hcp_name": r"\*\*\s*Name:\s*([^*]+?)(?:\*\*|Created|$)",
            "first_name": r"\*\*\s*First Name:\s*([^*]+?)(?:\*\*|Created|$)",
            "last_name": r"\*\*\s*Last Name:\s*([^*]+?)(?:\*\*|Created|$)",
            "middle_name": r"\*\*\s*Middle Names?:\s*([^*]+?)(?:\*\*|Created|$)",
            "salutation": r"\*\*\s*Salutation:\s*([^*]+?)(?:\*\*|Created|$)",
            "title": r"\*\*\s*Title:\s*([^*]+?)(?:\*\*|Created|$)",
            "specialty": r"\*\*\s*Specialties:\s*([^*]+?)(?:\*\*|Created|$)",
            "qualifications": r"\*\*\s*Qualifications:\s*([^*]+?)(?:\*\*|Created|$)",
            "department": r"\*\*\s*Department:\s*([^*]+?)(?:\*\*|Created|$)",
        }
        for key, pattern in patterns.items():
            match = re.search(pattern, note_clean, re.IGNORECASE)
            if match:
                result[key] = match.group(1).strip()

        return result

    @classmethod
    def parse(cls, note, source=None):
        fmt = cls.detect_format(note)
        if fmt == "manual":
            return cls.parse_manual(note)
        elif fmt == "omnia":
            return cls.parse_omnia(note)
        else:
            manual = cls.parse_manual(note)
            omnia = cls.parse_omnia(note)
            manual_hits = sum(1 for v in manual.values() if v and v != "manual")
            omnia_hits = sum(1 for v in omnia.values() if v and v != "omnia")
            return omnia if omnia_hits > manual_hits else manual


# =============================================================================
# VALIDATORS
# =============================================================================

class Validators:
    @staticmethod
    def check_completeness(pdr_row, parsed_note):
        missing = []
        first_name = str(pdr_row.get("first_name", "")).strip()
        last_name = str(pdr_row.get("last_name", "")).strip()
        hcp_name = parsed_note.get("hcp_name", "")
        if (not first_name or first_name.lower() == "nan") and \
           (not last_name or last_name.lower() == "nan") and (not hcp_name):
            missing.append("HCP Name")
        if not parsed_note.get("hco_url", ""):
            missing.append("Source URL (POV)")
        if not parsed_note.get("hco_vid", ""):
            missing.append("HCO VID")
        return (len(missing) == 0, missing)

    @staticmethod
    def check_scope(parsed_note, pdr_row=None):
        text_sources = []
        for field in ["qualifications", "specialty", "department", "title"]:
            val = parsed_note.get(field, "")
            if val:
                text_sources.append(val)
        combined_text = " ".join(text_sources).lower()

        if not combined_text.strip():
            scope_status = parsed_note.get("scope_status", "")
            if scope_status and "in scope" in scope_status.lower():
                return (True, "Pre-marked 'In scope'")
            return (None, "No qualification data - manual review needed")

        for oos in Config.OUT_OF_SCOPE_SPECIALTIES:
            if oos.lower() in combined_text:
                return (False, f"Out of scope: '{oos}' detected")
        for ins in Config.IN_SCOPE_DEGREES:
            if ins.lower() in combined_text:
                return (True, f"In scope: '{ins.upper()}' confirmed")
        for title in Config.BUSINESS_TITLES:
            if title.lower() in combined_text:
                return (True, f"Business Professional: '{title}'")
        return (None, f"Scope uncertain: '{combined_text[:60]}'")

    @staticmethod
    def check_junk_data(pdr_row, parsed_note):
        issues = []
        first_name = str(pdr_row.get("first_name", "")).strip()
        last_name = str(pdr_row.get("last_name", "")).strip()
        hcp_name = parsed_note.get("hcp_name", "")

        if first_name and ("," in first_name or " and " in first_name.lower() or ";" in first_name):
            issues.append(f"Multiple names in first_name: '{first_name}'")

        full_name = f"{first_name} {last_name}".strip()
        if len(full_name) < Config.MIN_NAME_LENGTH:
            issues.append(f"Name too short: '{full_name}'")
        if len(full_name) > Config.MAX_NAME_LENGTH:
            issues.append("Name too long - possibly scraped text")

        for pattern in Config.JUNK_PATTERNS:
            if re.search(pattern, first_name + " " + last_name + " " + (hcp_name or "")):
                issues.append(f"Junk pattern in name field")

        entity_patterns = [r"(?i)^(the\s+)?(association|society|council|federation|forum|committee|trust|foundation)\b"]
        for pattern in entity_patterns:
            if re.search(pattern, full_name) or (hcp_name and re.search(pattern, hcp_name)):
                issues.append("Non-individual entity in name")

        return (len(issues) == 0, issues)


# =============================================================================
# FUZZY MATCHER
# =============================================================================

class FuzzyMatcher:
    def __init__(self, nwk_df):
        self.nwk_df = nwk_df
        self.index = defaultdict(list)
        self._build_index()

    def _normalize(self, text):
        if not text or not isinstance(text, str):
            return ""
        text = re.sub(r"(?i)^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?)\s*", "", text)
        text = re.sub(r"[.\-',()]", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def _get_block_keys(self, first_name, last_name, city):
        keys = []
        fn = self._normalize(first_name)
        ln = self._normalize(last_name)
        c = self._normalize(city)
        if ln and len(ln) >= 2:
            ln_prefix = ln[:3]
            if c:
                keys.append(f"{ln_prefix}_{c}")
            keys.append(f"ln_{ln_prefix}")
        if fn and len(fn) >= 2:
            fn_prefix = fn[:3]
            if c:
                keys.append(f"{fn_prefix}_{c}")
        if fn and ln:
            keys.append(f"fl_{fn[:2]}_{ln[:2]}")
        return keys

    def _build_index(self):
        for idx, row in self.nwk_df.iterrows():
            fn = str(row.get("first_name", ""))
            ln = str(row.get("last_name", ""))
            city = str(row.get("city", ""))
            for key in self._get_block_keys(fn, ln, city):
                self.index[key].append(idx)

    def _similarity(self, s1, s2):
        if not s1 or not s2:
            return 0
        s1 = self._normalize(s1)
        s2 = self._normalize(s2)
        if not s1 or not s2:
            return 0
        if USE_RAPIDFUZZ:
            return int(rf_fuzz.ratio(s1, s2))
        return int(SequenceMatcher(None, s1, s2).ratio() * 100)

    def find_matches(self, first_name, last_name, specialty, city, affiliation, top_n=3):
        pdr_name = f"{first_name} {last_name}".strip()
        block_keys = self._get_block_keys(first_name, last_name, city)
        candidate_indices = set()
        for key in block_keys:
            candidate_indices.update(self.index.get(key, []))

        if not candidate_indices:
            return []

        scores = []
        for idx in candidate_indices:
            nwk_row = self.nwk_df.loc[idx]
            nwk_name = f"{nwk_row.get('first_name', '')} {nwk_row.get('last_name', '')}".strip()
            nwk_spec = str(nwk_row.get("specialty_1", ""))
            nwk_city = str(nwk_row.get("city", ""))
            nwk_affil = str(nwk_row.get("parent_hco_name", ""))

            name_score = self._similarity(pdr_name, nwk_name)
            spec_score = self._similarity(specialty, nwk_spec) if specialty else 50
            city_score = self._similarity(city, nwk_city) if city else 50
            affil_score = self._similarity(affiliation, nwk_affil) if affiliation else 50

            composite = int((name_score * 0.50) + (spec_score * 0.20) + (city_score * 0.15) + (affil_score * 0.15))

            scores.append({
                "composite": composite, "name_score": name_score,
                "specialty_score": spec_score, "city_score": city_score,
                "affiliation_score": affil_score, "nwk_name": nwk_name,
                "nwk_specialty": nwk_spec, "nwk_city": nwk_city,
                "nwk_affiliation": nwk_affil,
                "nwk_vid": str(nwk_row.get("vid", "")),
            })

        scores.sort(key=lambda x: x["composite"], reverse=True)
        return scores[:top_n]


# =============================================================================
# COLUMN MAPPING
# =============================================================================

PDR_COLUMN_MAP = {
    "hcp.vid__v (VID)": "vid",
    "hcp.first_name__v (FIRST NAME)": "first_name",
    "hcp.last_name__v (LAST NAME)": "last_name",
    "hcp.hcp_type__v (HCP TYPE)": "hcp_type",
    "change_request.change_request_id (CHANGE REQUEST ID)": "change_request_id",
    "change_request.source (SOURCE)": "source",
    "change_request_hcp.custom_comments__c_req (STEWARD NOTE (REQUESTED))": "steward_note",
    "change_request_hco.corporate_name__v_req (CORPORATE NAME (REQUESTED))": "corporate_name",
    "change_request.change_request_type (TYPE)": "type",
    "change_request.owner (OWNER)": "owner",
    "change_request.state_key (STATE KEY)": "state_key",
}

NWK_COLUMN_MAP = {
    "hcp.vid__v (VID)": "vid",
    "hcp.first_name__v (FIRST NAME)": "first_name",
    "hcp.last_name__v (LAST NAME)": "last_name",
    "hcp.primary_country__v (PRIMARY COUNTRY)": "country",
    "hcp.specialty_1__v (SPECIALTY 1)": "specialty_1",
    "hcp.hcp_status__v (STATUS)": "status",
    "hcp.source_full_name__v (SOURCE FULL NAME)": "source_full_name",
    "hcp.city_cda__v (CITY (CDA))": "city",
    "hcp.custom_degree_1__c (CUSTOM DEGREE 1)": "degree_1",
    "hcp.custom_degree_2__c (CUSTOM DEGREE 2)": "degree_2",
    "hcp.hcp_type__v (HCP TYPE)": "hcp_type",
    "hcp.gender__v (GENDER)": "gender",
    "hco.parent_hco_vid__v (PARENT_HCO_VID__V)": "parent_hco_vid",
    "hco.parent_hco_name__v (PARENT_HCO_NAME__V)": "parent_hco_name",
    "address.locality__v (CITY)": "address_city",
    "address.administrative_area__v (STATE/PROVINCE)": "state",
}


# =============================================================================
# PROCESSING FUNCTION
# =============================================================================

def process_pdrs(pdr_df, nwk_df, config, progress_callback=None):
    """Main processing pipeline. Returns results list and stats dict."""

    # Rename columns
    rename_pdr = {k: v for k, v in PDR_COLUMN_MAP.items() if k in pdr_df.columns}
    pdr_df = pdr_df.rename(columns=rename_pdr)

    rename_nwk = {}
    if nwk_df is not None and not nwk_df.empty:
        rename_nwk = {k: v for k, v in NWK_COLUMN_MAP.items() if k in nwk_df.columns}
        nwk_df = nwk_df.rename(columns=rename_nwk)

    total = len(pdr_df)
    results = []
    stats = {
        "total": total, "tier1_auto": 0, "tier2_assisted": 0, "tier3_human": 0,
        "r_10001": 0, "r_10058": 0, "r_10075": 0, "r_10056": 0,
        "a_10017": 0, "review": 0,
    }

    # Build matcher if NWK data available
    matcher = None
    if nwk_df is not None and not nwk_df.empty:
        matcher = FuzzyMatcher(nwk_df)

    for i, (idx, row) in enumerate(pdr_df.iterrows()):
        if progress_callback:
            progress_callback(i / total, f"Processing PDR {i+1} of {total}")

        r = {
            "vid": str(row.get("vid", "")),
            "first_name": str(row.get("first_name", "")),
            "last_name": str(row.get("last_name", "")),
            "change_request_id": str(row.get("change_request_id", "")),
            "source": str(row.get("source", "")),
            "ai_resolution_code": "",
            "ai_confidence": 0,
            "ai_tier": "",
            "match_vid": "",
            "match_score": 0,
            "match_details": "",
            "recommendation_notes": "",
            "steward_action": "",
            "parsed_hcp_name": "",
            "parsed_specialty": "",
            "parsed_qualifications": "",
            "parsed_hco_vid": "",
            "parsed_hco_name": "",
            "parsed_hco_url": "",
            "parsed_city": "",
            "note_format": "",
            "scope_check": "",
        }

        # Parse steward note
        note = row.get("steward_note", "")
        parsed = NoteParser.parse(note)
        r["parsed_hcp_name"] = parsed.get("hcp_name", "") or ""
        r["parsed_specialty"] = parsed.get("specialty", "") or parsed.get("department", "") or ""
        r["parsed_qualifications"] = parsed.get("qualifications", "") or ""
        r["parsed_hco_vid"] = parsed.get("hco_vid", "") or ""
        r["parsed_hco_name"] = parsed.get("hco_name", "") or ""
        r["parsed_hco_url"] = parsed.get("hco_url", "") or ""
        r["parsed_city"] = parsed.get("city", "") or ""
        r["note_format"] = parsed.get("parse_format", "") or ""

        # Gate 1: Completeness
        is_complete, missing = Validators.check_completeness(row, parsed)
        if not is_complete:
            r["ai_resolution_code"] = "R-10001"
            r["ai_confidence"] = 95
            r["ai_tier"] = "Tier 1 (Auto)"
            r["recommendation_notes"] = f"Incomplete: missing {', '.join(missing)}"
            r["steward_action"] = "SPOT_CHECK"
            stats["r_10001"] += 1
            stats["tier1_auto"] += 1
            results.append(r)
            continue

        # Gate 2: Scope
        is_in_scope, reason = Validators.check_scope(parsed, row)
        r["scope_check"] = "In Scope" if is_in_scope else ("Out of Scope" if is_in_scope is False else "Uncertain")

        if is_in_scope is False:
            r["ai_resolution_code"] = "R-10058"
            r["ai_confidence"] = 90
            r["ai_tier"] = "Tier 1 (Auto)"
            r["recommendation_notes"] = reason
            r["steward_action"] = "SPOT_CHECK"
            stats["r_10058"] += 1
            stats["tier1_auto"] += 1
            results.append(r)
            continue

        # Gate 3: Junk data
        is_clean, issues = Validators.check_junk_data(row, parsed)
        if not is_clean:
            r["ai_resolution_code"] = "R-10056"
            r["ai_confidence"] = 85
            r["ai_tier"] = "Tier 1 (Auto)"
            r["recommendation_notes"] = f"Junk data: {'; '.join(issues)}"
            r["steward_action"] = "SPOT_CHECK"
            stats["r_10056"] += 1
            stats["tier1_auto"] += 1
            results.append(r)
            continue

        # Gate 4: Duplicate matching
        if matcher:
            first_name = str(row.get("first_name", "")).strip()
            last_name = str(row.get("last_name", "")).strip()
            specialty = r["parsed_specialty"]
            city = r["parsed_city"]
            affiliation = r["parsed_hco_name"] or str(row.get("corporate_name", ""))

            matches = matcher.find_matches(first_name, last_name, specialty, city, affiliation)

            if matches:
                best = matches[0]
                r["match_vid"] = best["nwk_vid"]
                r["match_score"] = best["composite"]
                r["match_details"] = (
                    f"PDR: {first_name} {last_name} | NWK: {best['nwk_name']} | "
                    f"Name:{best['name_score']}% Spec:{best['specialty_score']}% "
                    f"City:{best['city_score']}% Affil:{best['affiliation_score']}%"
                )

                if best["composite"] >= config["exact_threshold"]:
                    r["ai_resolution_code"] = "R-10075"
                    r["ai_confidence"] = best["composite"]
                    r["ai_tier"] = "Tier 1 (Auto)"
                    r["recommendation_notes"] = f"Exact match: {best['nwk_name']} (VID: {best['nwk_vid']})"
                    r["steward_action"] = "SPOT_CHECK"
                    stats["r_10075"] += 1
                    stats["tier1_auto"] += 1
                elif best["composite"] >= config["partial_threshold"]:
                    r["ai_resolution_code"] = "REVIEW"
                    r["ai_confidence"] = best["composite"]
                    r["ai_tier"] = "Tier 2 (AI-Assisted)"
                    diffs = []
                    if best["name_score"] < 90:
                        diffs.append(f"Name ({best['name_score']}%)")
                    if best["specialty_score"] < 80:
                        diffs.append(f"Specialty ({best['specialty_score']}%)")
                    if best["affiliation_score"] < 70:
                        diffs.append(f"Affiliation ({best['affiliation_score']}%)")
                    r["recommendation_notes"] = (
                        f"Partial match: {best['nwk_name']} (VID: {best['nwk_vid']}). "
                        f"Diffs: {', '.join(diffs) if diffs else 'minor'}. May need PV."
                    )
                    r["steward_action"] = "REVIEW_MATCH"
                    stats["review"] += 1
                    stats["tier2_assisted"] += 1
                else:
                    r["ai_resolution_code"] = "A-10017"
                    r["ai_confidence"] = 75
                    r["ai_tier"] = "Tier 2 (AI-Assisted)"
                    r["recommendation_notes"] = f"No strong match (best: {best['nwk_name']}, {best['composite']}%). New HCP."
                    r["steward_action"] = "CREATE_RECORD"
                    stats["a_10017"] += 1
                    stats["tier2_assisted"] += 1
            else:
                r["ai_resolution_code"] = "A-10017"
                r["ai_confidence"] = 80
                r["ai_tier"] = "Tier 2 (AI-Assisted)"
                r["recommendation_notes"] = "No match in NWK. New HCP — validate URL and create."
                r["steward_action"] = "CREATE_RECORD"
                stats["a_10017"] += 1
                stats["tier2_assisted"] += 1
        else:
            r["ai_resolution_code"] = "REVIEW"
            r["ai_confidence"] = 50
            r["ai_tier"] = "Tier 2 (AI-Assisted)"
            r["recommendation_notes"] = "No NWK data — manual search required"
            r["steward_action"] = "REVIEW_MATCH"
            stats["review"] += 1
            stats["tier2_assisted"] += 1

        results.append(r)

    if progress_callback:
        progress_callback(1.0, "Processing complete!")

    return results, stats


# =============================================================================
# SIDEBAR — CONFIGURATION & FILE UPLOAD
# =============================================================================

render_sidebar_nav(app_title="PDR Processor", subtitle="GK.Ai", version="v1.0")

# File uploads
st.sidebar.markdown("### 📁 Data Files")
pdr_file = st.sidebar.file_uploader("Upload PDR Export (.xlsx)", type=["xlsx", "csv"], key="pdr")

st.sidebar.markdown("**NWK Reference Data (Duplicate Check)**")
nwk_file = st.sidebar.file_uploader("Upload NWK file", type=["xlsx", "csv"], key="nwk")
nwk_local_path = st.sidebar.text_input(
    "Or paste file path (no size limit)",
    placeholder="/Users/GopalakrishnanK B/Downloads/nwk_export.csv",
    help="For large files (800MB+), paste the path here instead of uploading. Loads directly from disk."
)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Threshold Tuning")

exact_threshold = st.sidebar.slider(
    "Exact Match Threshold",
    min_value=80, max_value=100, value=92, step=1,
    help="Above this score = auto-reject as duplicate (R-10075)"
)

partial_threshold = st.sidebar.slider(
    "Partial Match Threshold",
    min_value=50, max_value=90, value=70, step=1,
    help="Between this and exact = flag for steward review"
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Processing Info")
if USE_RAPIDFUZZ:
    st.sidebar.success("✅ rapidfuzz installed (fast mode)")
else:
    st.sidebar.warning("⚠️ Using difflib (install rapidfuzz for 10x speed)")

if USE_PLOTLY:
    st.sidebar.success("✅ plotly installed (charts enabled)")
else:
    st.sidebar.warning("⚠️ Install plotly for dashboard charts")

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<small>🔒 100% local processing<br>No data leaves this machine</small>",
    unsafe_allow_html=True
)


# =============================================================================
# MAIN AREA
# =============================================================================

render_app_header(
    title="PDR Processing Dashboard",
    description="AI-Augmented PDR triage tool with interactive dashboard",
    tags=[{"label": "AI-Augmented", "color": "blue"}],
)

# Step-by-step instructions
if pdr_file is None:
    st.markdown("""
    ### How to Use

    **Step 1:** Export your PDR batch from Veeva as an Excel file and upload it in the sidebar.

    **Step 2 (optional but recommended):** Export your NWK reference data (existing HCPs) and upload it for duplicate matching.

    **Step 3:** Adjust the matching thresholds in the sidebar if needed.

    **Step 4:** Click **Process PDRs** to run the AI triage pipeline.

    **Step 5:** Review the results in the interactive table, check the dashboard, and download the enriched output.

    ---

    **Threshold Guide:**
    - **Exact Match (default 92):** PDRs scoring above this against NWK are auto-rejected as duplicates. Higher = more conservative (fewer auto-rejects).
    - **Partial Match (default 70):** PDRs scoring between this and the exact threshold are flagged for steward review. Lower = more records auto-classified as "new HCP".
    """)
    st.stop()

# Load data
@st.cache_data
def load_file(uploaded_file):
    if uploaded_file.name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)

# Only the columns needed for matching — cuts 800MB file down to ~60-80MB in memory
NWK_REQUIRED_COLUMNS = list(NWK_COLUMN_MAP.keys())

@st.cache_data
def load_nwk_from_path(file_path):
    """Load NWK file from local disk path, reading only the columns needed for matching."""
    import os
    if not os.path.exists(file_path):
        return None, f"File not found: {file_path}"
    try:
        if file_path.endswith(".csv"):
            # For CSV: read only needed columns
            all_cols = pd.read_csv(file_path, nrows=0).columns.tolist()
            use_cols = [c for c in NWK_REQUIRED_COLUMNS if c in all_cols]
            return pd.read_csv(file_path, usecols=use_cols), None
        else:
            # For Excel: read only needed columns
            all_cols = pd.read_excel(file_path, nrows=0, engine="openpyxl").columns.tolist()
            use_cols = [c for c in NWK_REQUIRED_COLUMNS if c in all_cols]
            return pd.read_excel(file_path, usecols=use_cols, engine="openpyxl"), None
    except Exception as e:
        return None, str(e)

pdr_df = load_file(pdr_file)
st.success(f"✅ PDR file loaded: **{len(pdr_df)}** records")

nwk_df = None
if nwk_local_path:
    # Local path takes priority (no size limit)
    with st.spinner(f"Loading NWK from disk (only matching columns — saves ~90% memory)..."):
        nwk_df, error = load_nwk_from_path(nwk_local_path)
    if error:
        st.error(f"Failed to load NWK file: {error}")
    elif nwk_df is not None:
        st.success(f"✅ NWK reference loaded from disk: **{len(nwk_df)}** records ({len(nwk_df.columns)} columns)")
elif nwk_file:
    # Upload fallback for smaller files
    nwk_df = load_file(nwk_file)
    st.success(f"✅ NWK reference uploaded: **{len(nwk_df)}** records")
else:
    st.warning("⚠️ No NWK file provided — duplicate matching will be skipped")

# Process button
st.markdown("---")

if st.button("🚀 Process PDRs", type="primary", use_container_width=True):

    config = {
        "exact_threshold": exact_threshold,
        "partial_threshold": partial_threshold,
    }

    progress_bar = st.progress(0)
    status_text = st.empty()

    def update_progress(pct, msg):
        progress_bar.progress(pct)
        status_text.text(msg)

    start_time = time.time()
    results, stats = process_pdrs(pdr_df, nwk_df, config, progress_callback=update_progress)
    elapsed = time.time() - start_time

    progress_bar.progress(1.0)
    status_text.text(f"✅ Processed {stats['total']} PDRs in {elapsed:.1f}s")

    # Store in session state
    st.session_state["results"] = results
    st.session_state["stats"] = stats
    st.session_state["elapsed"] = elapsed


# =============================================================================
# RESULTS DISPLAY
# =============================================================================

if "results" in st.session_state:
    results = st.session_state["results"]
    stats = st.session_state["stats"]
    elapsed = st.session_state["elapsed"]

    results_df = pd.DataFrame(results)

    # ---- TAB LAYOUT ----
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📋 Results Table", "⬇️ Export"])

    # ============ TAB 1: DASHBOARD ============
    with tab1:
        st.markdown("### Processing Summary")

        # KPI metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total PDRs", stats["total"])
        with col2:
            st.metric("Tier 1 (Auto)", stats["tier1_auto"],
                       delta=f"{stats['tier1_auto']/max(stats['total'],1)*100:.0f}%")
        with col3:
            st.metric("Tier 2 (Assisted)", stats["tier2_assisted"],
                       delta=f"{stats['tier2_assisted']/max(stats['total'],1)*100:.0f}%")
        with col4:
            st.metric("Tier 3 (Human)", stats["tier3_human"],
                       delta=f"{stats['tier3_human']/max(stats['total'],1)*100:.0f}%")
        with col5:
            time_saved = (stats["tier1_auto"] * 5) + (stats["tier2_assisted"] * 4)
            st.metric("Time Saved", f"{time_saved} min", delta=f"{time_saved/60:.1f} hrs")

        st.markdown("---")

        # Charts
        if USE_PLOTLY:
            col_chart1, col_chart2 = st.columns(2)

            with col_chart1:
                # Tier distribution pie chart
                tier_data = {
                    "Tier": ["Tier 1 (Auto)", "Tier 2 (AI-Assisted)", "Tier 3 (Human)"],
                    "Count": [stats["tier1_auto"], stats["tier2_assisted"], stats["tier3_human"]],
                }
                fig1 = px.pie(
                    tier_data, values="Count", names="Tier",
                    color="Tier",
                    color_discrete_map={
                        "Tier 1 (Auto)": "#1e8449",
                        "Tier 2 (AI-Assisted)": "#e67e22",
                        "Tier 3 (Human)": "#c0392b",
                    },
                    title="Processing Tier Distribution",
                    hole=0.4,
                )
                fig1.update_layout(height=350)
                st.plotly_chart(fig1, use_container_width=True)

            with col_chart2:
                # Resolution code breakdown
                code_data = {
                    "Code": ["R-10001", "R-10058", "R-10056", "R-10075", "A-10017", "REVIEW"],
                    "Count": [
                        stats["r_10001"], stats["r_10058"], stats["r_10056"],
                        stats["r_10075"], stats["a_10017"], stats["review"],
                    ],
                    "Color": ["#e74c3c", "#e67e22", "#9b59b6", "#3498db", "#2ecc71", "#f1c40f"],
                }
                fig2 = px.bar(
                    code_data, x="Code", y="Count",
                    color="Code",
                    color_discrete_sequence=["#e74c3c", "#e67e22", "#9b59b6", "#3498db", "#2ecc71", "#f1c40f"],
                    title="Resolution Code Breakdown",
                )
                fig2.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            # Time savings projection
            st.markdown("### Sprint Projection")
            col_proj1, col_proj2, col_proj3 = st.columns(3)

            total_pdrs = stats["total"]
            auto_rate = stats["tier1_auto"] / max(total_pdrs, 1)

            with col_proj1:
                projected_auto = int(9000 * auto_rate)
                st.metric("Projected Tier 1 (of 9K)", f"{projected_auto}")

            with col_proj2:
                projected_steward = 9000 - projected_auto
                st.metric("Steward-Touched PDRs", f"{projected_steward}")

            with col_proj3:
                projected_time_saved = (projected_auto * 5) + (projected_steward * 4)
                st.metric("Projected Time Saved", f"{projected_time_saved/60:.0f} hours")

        else:
            # Fallback without plotly
            st.markdown("#### Resolution Code Breakdown")
            code_display = {
                "R-10001 (Incomplete)": stats["r_10001"],
                "R-10058 (Out of Scope)": stats["r_10058"],
                "R-10056 (Junk Data)": stats["r_10056"],
                "R-10075 (Exact Match)": stats["r_10075"],
                "A-10017 (New HCP)": stats["a_10017"],
                "REVIEW (Partial Match)": stats["review"],
            }
            for code, count in code_display.items():
                st.text(f"  {code}: {count}")

    # ============ TAB 2: RESULTS TABLE ============
    with tab2:
        st.markdown("### Interactive Results")

        # Filters
        filter_col1, filter_col2, filter_col3 = st.columns(3)

        with filter_col1:
            tier_filter = st.multiselect(
                "Filter by Tier",
                options=results_df["ai_tier"].unique().tolist(),
                default=results_df["ai_tier"].unique().tolist(),
            )

        with filter_col2:
            code_filter = st.multiselect(
                "Filter by Resolution Code",
                options=results_df["ai_resolution_code"].unique().tolist(),
                default=results_df["ai_resolution_code"].unique().tolist(),
            )

        with filter_col3:
            action_filter = st.multiselect(
                "Filter by Steward Action",
                options=results_df["steward_action"].unique().tolist(),
                default=results_df["steward_action"].unique().tolist(),
            )

        # Apply filters
        filtered_df = results_df[
            (results_df["ai_tier"].isin(tier_filter)) &
            (results_df["ai_resolution_code"].isin(code_filter)) &
            (results_df["steward_action"].isin(action_filter))
        ]

        st.markdown(f"**Showing {len(filtered_df)} of {len(results_df)} records**")

        # Display columns
        display_cols = [
            "first_name", "last_name", "ai_resolution_code", "ai_confidence",
            "ai_tier", "steward_action", "match_vid", "match_score",
            "recommendation_notes", "parsed_hcp_name", "parsed_city",
            "parsed_hco_name", "parsed_specialty", "note_format",
        ]

        available_cols = [c for c in display_cols if c in filtered_df.columns]
        st.dataframe(
            filtered_df[available_cols],
            use_container_width=True,
            height=500,
            column_config={
                "ai_confidence": st.column_config.ProgressColumn(
                    "Confidence", min_value=0, max_value=100, format="%d%%"
                ),
                "match_score": st.column_config.ProgressColumn(
                    "Match Score", min_value=0, max_value=100, format="%d%%"
                ),
            }
        )

        # Expandable detail view
        st.markdown("### Record Detail View")
        selected_idx = st.selectbox(
            "Select a record to view details",
            range(len(filtered_df)),
            format_func=lambda x: f"{filtered_df.iloc[x]['first_name']} {filtered_df.iloc[x]['last_name']} — {filtered_df.iloc[x]['ai_resolution_code']}"
        )

        if selected_idx is not None:
            record = filtered_df.iloc[selected_idx]
            col_detail1, col_detail2 = st.columns(2)

            with col_detail1:
                st.markdown("**PDR Data**")
                st.text(f"Name: {record['first_name']} {record['last_name']}")
                st.text(f"Parsed Name: {record['parsed_hcp_name']}")
                st.text(f"City: {record['parsed_city']}")
                st.text(f"HCO: {record['parsed_hco_name']}")
                st.text(f"Specialty: {record['parsed_specialty']}")
                st.text(f"Qualifications: {record.get('parsed_qualifications', 'N/A')}")
                st.text(f"Note Format: {record['note_format']}")

            with col_detail2:
                tier = record['ai_tier']
                if "Tier 1" in str(tier):
                    st.markdown(f'<div class="tier1"><strong>{tier}</strong></div>', unsafe_allow_html=True)
                elif "Tier 2" in str(tier):
                    st.markdown(f'<div class="tier2"><strong>{tier}</strong></div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="tier3"><strong>{tier}</strong></div>', unsafe_allow_html=True)

                st.text(f"Resolution: {record['ai_resolution_code']}")
                st.text(f"Confidence: {record['ai_confidence']}%")
                st.text(f"Action: {record['steward_action']}")
                st.text(f"Match VID: {record['match_vid'] or 'N/A'}")
                st.text(f"Match Score: {record['match_score']}%")
                st.markdown(f"**Notes:** {record['recommendation_notes']}")

            if record.get("match_details"):
                st.markdown(f"**Match Details:** `{record['match_details']}`")

    # ============ TAB 3: EXPORT ============
    with tab3:
        st.markdown("### Export Processed Results")

        # Full export
        output = io.BytesIO()
        results_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        st.download_button(
            label="📥 Download Full Results (.xlsx)",
            data=output,
            file_name=f"PDR_Processed_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.ml",
            type="primary",
            use_container_width=True,
        )

        st.markdown("---")

        # Tier-specific exports
        st.markdown("### Export by Tier")
        col_exp1, col_exp2, col_exp3 = st.columns(3)

        with col_exp1:
            tier1_df = results_df[results_df["ai_tier"].str.contains("Tier 1", na=False)]
            if not tier1_df.empty:
                buf = io.BytesIO()
                tier1_df.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button(
                    f"Tier 1 Auto-Resolved ({len(tier1_df)})",
                    data=buf,
                    file_name=f"PDR_Tier1_AutoResolved_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.ml",
                )

        with col_exp2:
            tier2_df = results_df[results_df["ai_tier"].str.contains("Tier 2", na=False)]
            if not tier2_df.empty:
                buf = io.BytesIO()
                tier2_df.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button(
                    f"Tier 2 AI-Assisted ({len(tier2_df)})",
                    data=buf,
                    file_name=f"PDR_Tier2_Assisted_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.ml",
                )

        with col_exp3:
            tier3_df = results_df[results_df["ai_tier"].str.contains("Tier 3", na=False)]
            if not tier3_df.empty:
                buf = io.BytesIO()
                tier3_df.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button(
                    f"Tier 3 Human Required ({len(tier3_df)})",
                    data=buf,
                    file_name=f"PDR_Tier3_Human_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.ml",
                )

        st.markdown("---")
        st.markdown("### Processing Stats")
        st.json(stats)

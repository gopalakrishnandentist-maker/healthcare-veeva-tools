"""
app.py — Streamlit GUI for the HCP/HCO Duplicate Identification Tool.

Launch with:
    streamlit run app.py

Modes:
    1. Batch Dupe Check — Full duplicate detection on a Veeva extract
    2. Check Before Create — DS looks up one HCP against existing DB
    3. PDR Pre-Screen — Screen a batch of new PDR records vs existing DB
"""

from __future__ import annotations

import io
import os
import sys
import time
import logging
from collections import Counter

import pandas as pd
import streamlit as st

# ── Ensure sibling modules are importable (works regardless of folder name) ──
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
if PACKAGE_DIR not in sys.path:
    sys.path.insert(0, PACKAGE_DIR)

from core import (
    BlockingEngine,
    DSU,
    SharedContactDetector,
    name_similarity,
    norm_text,
    remap_hco_columns,
)
from hcp_pipeline import run_hcp_pipeline
from hco_pipeline import run_hco_pipeline
from lookup import build_reference_index, lookup_single, screen_pdr_batch
from cross_match import (
    detect_header_row,
    auto_detect_columns,
    cross_match_batch,
    get_missing_data_warnings,
)
from run import _load_config, _hardcoded_defaults
from output import write_rules_sheet, build_tagged_source, _format_vid_columns_as_text
import pickle
import hashlib

# ── Reference DB Cache ────────────────────────────────────────────────
_CACHE_DIR = os.path.join(PACKAGE_DIR, ".cache")


def _ref_cache_path(source_hint: str = "default") -> str:
    """Return path to the cached reference index pickle file."""
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"ref_index_{source_hint}.pkl")


def _save_ref_index(ref_index, source_hint: str = "default") -> None:
    """Persist reference index to disk for fast reload across sessions."""
    path = _ref_cache_path(source_hint)
    try:
        with open(path, "wb") as f:
            pickle.dump(ref_index, f, protocol=pickle.HIGHEST_PROTOCOL)
        logging.getLogger("dupe_tool.app").info("Cached reference index → %s", path)
    except Exception as e:
        logging.getLogger("dupe_tool.app").warning("Failed to cache reference index: %s", e)


def _load_ref_index(source_hint: str = "default"):
    """Load reference index from disk cache. Returns None if unavailable."""
    path = _ref_cache_path(source_hint)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "rb") as f:
            ref_index = pickle.load(f)
        logging.getLogger("dupe_tool.app").info("Loaded cached reference index ← %s", path)
        return ref_index
    except Exception as e:
        logging.getLogger("dupe_tool.app").warning("Failed to load cached reference index: %s", e)
        return None


def _clear_ref_cache(source_hint: str = "default") -> None:
    """Delete cached reference index from disk."""
    path = _ref_cache_path(source_hint)
    if os.path.isfile(path):
        os.remove(path)


def _ref_cache_age(source_hint: str = "default") -> str | None:
    """Return human-readable age of the cache file, or None if no cache."""
    path = _ref_cache_path(source_hint)
    if not os.path.isfile(path):
        return None
    import datetime
    mtime = os.path.getmtime(path)
    age = datetime.datetime.now() - datetime.datetime.fromtimestamp(mtime)
    if age.days > 0:
        return f"{age.days}d ago"
    hours = age.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    minutes = age.seconds // 60
    return f"{minutes}m ago"

# ── GK.Ai shared theme ───────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
from gkai_theme import inject_gkai_theme, GKAI_PAGE_CONFIG, render_app_header, render_sidebar_nav

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    **GKAI_PAGE_CONFIG,
    page_title="HCP/HCO Dupe Check",
    page_icon="",
)
inject_gkai_theme()

# ── Header ───────────────────────────────────────────────────────────
render_app_header(
    title="HCP / HCO Duplicate Identification Tool",
    description="Detect duplicates, check before creating records, pre-screen PDR batches, and cross-match external lists",
)


# ── Sidebar: Configuration ───────────────────────────────────────────
with st.sidebar:
    render_sidebar_nav(app_title="HCP/HCO Dupe Check", subtitle="GK.Ai", version="v3.0")

    st.markdown("#### Profile Type")
    profile_type_selection = st.selectbox(
        "Entity profile",
        options=["Auto-detect", "HCP", "HCO", "Stockist"],
        index=0,
        help=(
            "Select the type of entity in your data. "
            "'Auto-detect' infers the type from column names and HCO type values. "
            "'Stockist' applies special name normalization to strip common distributor "
            "prefixes/suffixes (M/S, Medical Agency, etc.) that inflate similarity scores."
        ),
        key="profile_type_selector",
    )

    _default_stockist_prefixes = "m s, ms, messrs, shri, smt, sri, mr, mrs"
    _default_stockist_suffixes = (
        "medical agency, medical agencies, medical store, medical stores, "
        "medical hall, pharma, pharma distributors, pharmaceutical, pharmaceuticals, "
        "distributors, distributor, agencies, agency, enterprises, enterprise, "
        "sales, sales centre, sales center, traders, trading, trading co, "
        "company, co, pvt ltd, pvt, ltd, private limited, limited"
    )
    if profile_type_selection == "Stockist":
        with st.expander("Stockist Name Normalization", expanded=False):
            st.caption(
                "Prefixes and suffixes stripped from HCO names before comparison. "
                "Edit these comma-separated lists to customize."
            )
            stockist_prefixes_raw = st.text_area(
                "Prefixes to strip", value=_default_stockist_prefixes, height=68,
                key="stockist_prefixes",
            )
            stockist_suffixes_raw = st.text_area(
                "Suffixes to strip", value=_default_stockist_suffixes, height=100,
                key="stockist_suffixes",
            )
    else:
        stockist_prefixes_raw = _default_stockist_prefixes
        stockist_suffixes_raw = _default_stockist_suffixes

    st.markdown("---")
    st.markdown("#### Name Matching")
    name_strong = st.slider("Strong name threshold (%)", 80, 100, 92, help="Minimum similarity for 'strong' name match")
    name_medium = st.slider("Medium name threshold (%)", 70, 95, 85, help="Minimum similarity for 'medium' name match")

    st.markdown("---")
    st.markdown("#### Shared Contact Detection")
    shared_threshold = st.slider(
        "Shared contact threshold (VIDs)", 2, 20, 5,
        help="A phone/email appearing on this many VIDs is treated as 'shared' (e.g., hospital front desk)"
    )

    st.markdown("---")
    st.markdown("**Blocking**")
    max_block = st.slider("Max block size", 100, 1000, 500, help="Cap on block size to prevent combinatorial explosion")
    phonetic = st.checkbox("Enable Soundex blocking", value=True, help="Catches spelling variations like Krishnan/Krishan")
    first_initial = st.checkbox("Enable first-initial blocking", value=True, help="Broader recall via first initial + last name")

    st.markdown("---")
    st.markdown("#### Review Scoring")
    review_threshold = st.slider("Minimum review score", 30, 80, 50, help="Pairs scoring below this go to NOT-DUP")

    st.markdown("---")
    st.markdown("#### Effort Avoidance")
    manual_velocity = st.number_input(
        "Manual review velocity (VIDs/hr)",
        min_value=1.0, max_value=50.0, value=6.5, step=0.5,
        help="Your team's current throughput for manually reviewing suspect VIDs. Used to calculate hours saved."
    )

    st.markdown("---")
    st.markdown("#### About")
    st.caption("v5.0 | Built for Veeva OpenData extracts")
    st.caption("Modes: Batch | Check Before Create | PDR Pre-Screen | Cross-DB Match")


# ── Session state ────────────────────────────────────────────────────
if "results" not in st.session_state:
    st.session_state.results = None
if "hco_results" not in st.session_state:
    st.session_state.hco_results = None
if "run_time" not in st.session_state:
    st.session_state.run_time = None
if "ref_index" not in st.session_state:
    st.session_state.ref_index = None
    # Auto-load from disk cache on fresh session start
    _cached_ref = _load_ref_index()
    if _cached_ref is not None:
        st.session_state.ref_index = _cached_ref
if "lookup_results" not in st.session_state:
    st.session_state.lookup_results = None
if "pdr_results" not in st.session_state:
    st.session_state.pdr_results = None
if "xmatch_results" not in st.session_state:
    st.session_state.xmatch_results = None


# ── Helper: build config from sidebar ────────────────────────────────
def _build_cfg():
    cfg = _hardcoded_defaults()
    cfg["shared_contact"]["threshold"] = shared_threshold
    cfg["name_matching"]["strong"] = name_strong
    cfg["name_matching"]["medium"] = name_medium
    cfg["hcp_review_scoring"]["review_threshold"] = review_threshold
    cfg["manual_review"]["pairs_per_hour"] = manual_velocity
    cfg["output"]["enrich_output"] = True
    cfg["blocking"]["max_block_size"] = max_block
    cfg["blocking"]["phonetic_blocking"] = phonetic
    cfg["blocking"]["first_initial_blocking"] = first_initial
    cfg["blocking"]["max_pairs"] = 0

    # Profile type
    if profile_type_selection == "HCP":
        cfg["profile_type"] = "hcp"
    elif profile_type_selection == "HCO":
        cfg["profile_type"] = "hco"
    elif profile_type_selection == "Stockist":
        cfg["profile_type"] = "stockist"
        # Apply user-customized strip lists
        cfg.setdefault("profile_types", {}).setdefault("stockist", {})
        cfg["profile_types"]["stockist"]["strip_prefixes"] = [
            p.strip().lower() for p in stockist_prefixes_raw.split(",") if p.strip()
        ]
        cfg["profile_types"]["stockist"]["strip_suffixes"] = [
            s.strip().lower() for s in stockist_suffixes_raw.split(",") if s.strip()
        ]
    else:
        cfg["profile_type"] = "auto"

    return cfg


# ── Helper: load data ────────────────────────────────────────────────
@st.cache_data
def load_data_upload(file_bytes, filename):
    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    else:
        return pd.read_excel(io.BytesIO(file_bytes), dtype=str)


@st.cache_data
def load_data_path(fpath):
    if fpath.lower().endswith(".csv"):
        return pd.read_csv(fpath, dtype=str)
    else:
        return pd.read_excel(fpath, dtype=str)


# ── Helper: filter columns ───────────────────────────────────────────
def _filter_cols(df, cols):
    return [c for c in cols if c in df.columns]


# ══════════════════════════════════════════════════════════════════════
#  TOP-LEVEL MODE SELECTOR
# ══════════════════════════════════════════════════════════════════════

mode_tab_batch, mode_tab_lookup, mode_tab_pdr, mode_tab_xmatch = st.tabs([
    "Batch Dupe Check",
    "Check Before Create",
    "PDR Pre-Screen",
    "Cross-DB Match",
])


# ══════════════════════════════════════════════════════════════════════
#  MODE 1: BATCH DUPE CHECK (original functionality)
# ══════════════════════════════════════════════════════════════════════

with mode_tab_batch:

    # ── File Input ────────────────────────────────────────────────────
    input_method = st.radio(
        "How do you want to load your data?",
        ["Upload file (< 200MB)", "Enter file path (for large files)"],
        horizontal=True,
        key="batch_input_method",
    )

    uploaded_file = None
    file_path_input = None

    if input_method == "Upload file (< 200MB)":
        col_upload, col_info = st.columns([2, 1])
        with col_upload:
            uploaded_file = st.file_uploader(
                "Upload your HCP or HCO extract",
                type=["csv", "xlsx", "xls"],
                help="Drag and drop your Veeva export file here",
                key="batch_uploader",
            )
        with col_info:
            if uploaded_file is None:
                st.markdown("""
                **Supported formats:** CSV, Excel

                **HCP pipeline** runs when `hcp.vid__v (VID)` is present.
                **HCO pipeline** runs when `hco.vid__v (VID)` is present.

                If both columns exist, both pipelines run automatically.
                """)
    else:
        file_path_input = st.text_input(
            "Full path to your CSV or Excel file",
            placeholder="/Users/you/Desktop/data/your_file.csv",
            help="Paste the full file path here.",
            key="batch_path",
        )
        if file_path_input and not os.path.isfile(file_path_input):
            st.warning(f"File not found: `{file_path_input}`")
            file_path_input = None

    # ── Run Pipeline ──────────────────────────────────────────────────
    has_data = uploaded_file is not None or (file_path_input is not None and file_path_input)

    if has_data:
        with st.spinner("Loading data..."):
            if uploaded_file is not None:
                df = load_data_upload(uploaded_file.getvalue(), uploaded_file.name)
            else:
                df = load_data_path(file_path_input)

        with st.expander(f"Data Preview -- {len(df):,} rows x {len(df.columns)} columns", expanded=False):
            st.dataframe(df.head(50), use_container_width=True, height=300)

        hcp_vid_col = "hcp.vid__v (VID)"
        hco_vid_col = "hco.vid__v (VID)"
        # Auto-rename common Veeva aliases (e.g. "NETWORK ID" → "VID")
        for _std, _prefix in [(hcp_vid_col, "hcp.vid__v"), (hco_vid_col, "hco.vid__v")]:
            if _std not in df.columns:
                _aliases = [c for c in df.columns if c.lower().startswith(_prefix)]
                if len(_aliases) == 1:
                    df = df.rename(columns={_aliases[0]: _std})
                    st.info(f"Auto-mapped column: `{_aliases[0]}` → `{_std}`")
        has_hcp = hcp_vid_col in df.columns
        has_hco = hco_vid_col in df.columns

        # Auto-rename alternate HCO column names (centralized mapping with fuzzy fallback)
        if has_hco:
            df, _hco_mapped = remap_hco_columns(df, _build_cfg())
            if _hco_mapped:
                st.info(f"Auto-mapped HCO columns: {', '.join(_hco_mapped)}")

        if not has_hcp and not has_hco:
            st.error(f"Neither HCP VID (`{hcp_vid_col}`) nor HCO VID (`{hco_vid_col}`) found.")
            st.stop()

        unique_hcp_vids = df[hcp_vid_col].nunique() if has_hcp else 0
        unique_hco_vids = df[hco_vid_col].nunique() if has_hco else 0
        phone_cols_ct = len([c for c in df.columns if c.startswith("hcp.phone_")])
        email_cols_ct = len([c for c in df.columns if c.startswith("hcp.email_")])

        row_count = len(df)
        is_large = row_count > 50_000
        is_very_large = row_count > 200_000

        if is_very_large:
            st.warning(f"Very large dataset detected ({row_count:,} rows). Performance mode enabled.")
        elif is_large:
            st.info(f"Large dataset detected ({row_count:,} rows). Tighter blocking defaults applied.")

        pipelines_detected = []
        if has_hcp:
            pipelines_detected.append(f"HCP ({unique_hcp_vids:,} VIDs)")
        if has_hco:
            pipelines_detected.append(f"HCO ({unique_hco_vids:,} VIDs)")
        st.info(f"Detected pipelines: **{' + '.join(pipelines_detected)}**")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Raw Rows", f"{len(df):,}")
        col2.metric("HCP VIDs", f"{unique_hcp_vids:,}" if has_hcp else "N/A")
        col3.metric("HCO VIDs", f"{unique_hco_vids:,}" if has_hco else "N/A")
        col4.metric("Phone/Email Cols", f"{phone_cols_ct} / {email_cols_ct}")

        st.markdown("---")
        run_clicked = st.button("Run Duplicate Detection", type="primary", use_container_width=True, key="batch_run")

        if run_clicked:
            cfg = _build_cfg()

            # Auto-detect profile type when set to "auto"
            if cfg.get("profile_type") == "auto":
                if has_hcp and not has_hco:
                    cfg["profile_type"] = "hcp"
                elif has_hco:
                    _auto_types = cfg.get("profile_types", {}).get("stockist", {}).get("auto_detect_types", [])
                    _detected_stockist = False
                    if _auto_types:
                        _type_candidates = [
                            "hco.hco_type__v (TYPE)", "hco.hco_type__v (HCO TYPE)",
                            "hco_type__v",
                        ]
                        for _tc in _type_candidates:
                            if _tc in df.columns:
                                _vals = df[_tc].dropna().str.strip().str.lower()
                                _match_count = _vals.apply(
                                    lambda v: any(st in v for st in _auto_types)
                                ).sum()
                                if len(_vals) > 0 and _match_count / len(_vals) > 0.5:
                                    cfg["profile_type"] = "stockist"
                                    _detected_stockist = True
                                    st.info("Auto-detected profile: **Stockist** (based on HCO type values)")
                                break
                    if not _detected_stockist:
                        cfg["profile_type"] = "hco"
                else:
                    cfg["profile_type"] = "hco"

            st.info(f"Profile: **{cfg['profile_type'].upper()}**")

            if is_very_large:
                cfg["blocking"]["max_block_size"] = min(max_block, 100)
                cfg["blocking"]["phonetic_blocking"] = False
                cfg["blocking"]["first_initial_blocking"] = False
                cfg["blocking"]["max_pairs"] = 300_000
            elif is_large:
                cfg["blocking"]["max_block_size"] = min(max_block, 300)
                cfg["blocking"]["phonetic_blocking"] = False
                cfg["blocking"]["first_initial_blocking"] = False
                cfg["blocking"]["max_pairs"] = 500_000

            t0 = time.time()
            progress_bar = st.progress(0, text="Initializing...")
            status_container = st.container()
            step_placeholder = status_container.empty()
            detail_placeholder = status_container.empty()
            elapsed_placeholder = status_container.empty()

            _progress_state = {"completed": [], "current": ""}

            def _render_steps(steps_done, active_step, detail_msg, pct):
                progress_bar.progress(min(pct, 99), text=active_step)
                elapsed = time.time() - t0
                lines = []
                for s in steps_done:
                    lines.append(f'<div class="progress-step done"><span class="step-dot done"></span>{s}</div>')
                if active_step:
                    lines.append(f'<div class="progress-step active"><span class="step-dot active"></span>{active_step}</div>')
                step_placeholder.markdown(
                    f'<div class="progress-container">{"".join(lines)}</div>',
                    unsafe_allow_html=True,
                )
                detail_placeholder.caption(f"{detail_msg}")
                elapsed_placeholder.caption(f"Elapsed: {elapsed:.0f}s")

            def update_progress(pct, msg):
                pct = min(pct, 99)
                _progress_state["current"] = msg
                _render_steps(_progress_state["completed"], msg, msg, pct)

            def complete_step(step_name):
                _progress_state["completed"].append(step_name)

            hcp_res = None
            if has_hcp:
                update_progress(2, "Starting HCP duplicate detection...")
                hcp_res = run_hcp_pipeline(df, cfg, progress_fn=update_progress)
                complete_step(f"HCP pipeline complete -- {len(hcp_res.get('hcp_auto', pd.DataFrame())):,} auto-merge, "
                              f"{len(hcp_res.get('hcp_review', pd.DataFrame())):,} review")

            hco_res = None
            if has_hco:
                update_progress(90, "Running HCO duplicate detection...")
                hco_res = run_hco_pipeline(df, cfg, progress_fn=update_progress)
                complete_step(f"HCO pipeline complete -- {len(hco_res.get('hco_auto', pd.DataFrame())):,} auto-merge, "
                              f"{len(hco_res.get('hco_review', pd.DataFrame())):,} review")

            update_progress(98, "Finalizing results...")
            time.sleep(0.3)
            complete_step("Results finalized")
            progress_bar.progress(100, text="Done!")

            elapsed = time.time() - t0
            _render_steps(_progress_state["completed"], "", f"Completed in {elapsed:.1f}s", 100)

            st.session_state.results = hcp_res
            st.session_state.hco_results = hco_res
            st.session_state.run_time = elapsed
            st.rerun()

    # ── Results Dashboard ─────────────────────────────────────────────
    _has_any_results = st.session_state.results is not None or st.session_state.hco_results is not None

    if _has_any_results:
        elapsed = st.session_state.run_time or 0
        st.success(f"Completed in {elapsed:.1f}s")

    # ── HCP Results ───────────────────────────────────────────────────
    if st.session_state.results is not None:
        results = st.session_state.results
        hcp_auto = results.get("hcp_auto", pd.DataFrame())
        hcp_review = results.get("hcp_review", pd.DataFrame())
        hcp_notdup = results.get("hcp_notdup", pd.DataFrame())
        hcp_unique = results.get("hcp_unique", pd.DataFrame())
        hcp_clusters = results.get("hcp_clusters", pd.DataFrame())
        hcp_shared = results.get("hcp_shared", pd.DataFrame())
        hcp_summary = results.get("hcp_summary", pd.DataFrame())

        auto_ct = len(hcp_auto)
        review_ct = len(hcp_review)
        notdup_ct = len(hcp_notdup)
        unique_ct = len(hcp_unique)
        cluster_ct = hcp_clusters["cluster_id"].nunique() if not hcp_clusters.empty and "cluster_id" in hcp_clusters.columns else 0
        total_pairs = auto_ct + review_ct + notdup_ct

        st.markdown('<div class="section-header">HCP Results Overview</div>', unsafe_allow_html=True)

        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        with c1:
            st.markdown(f'<div class="metric-card"><div class="metric-value vid-color">{hcp_summary.iloc[0].get("unique_hcp_vids", 0) if not hcp_summary.empty else 0:,.0f}</div><div class="metric-label">Unique VIDs</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="metric-card"><div class="metric-value pair-color">{total_pairs:,}</div><div class="metric-label">Pairs Evaluated</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="metric-card"><div class="metric-value auto-color">{auto_ct:,}</div><div class="metric-label">Auto-Merge</div></div>', unsafe_allow_html=True)
        with c4:
            st.markdown(f'<div class="metric-card"><div class="metric-value review-color">{review_ct:,}</div><div class="metric-label">Manual Review</div></div>', unsafe_allow_html=True)
        with c5:
            st.markdown(f'<div class="metric-card"><div class="metric-value notdup-color">{notdup_ct:,}</div><div class="metric-label">Not Duplicate</div></div>', unsafe_allow_html=True)
        with c6:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#38bdf8;">{unique_ct:,}</div><div class="metric-label">Unique (No Pairs)</div></div>', unsafe_allow_html=True)
        with c7:
            st.markdown(f'<div class="metric-card"><div class="metric-value cluster-color">{cluster_ct:,}</div><div class="metric-label">Clusters</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Effort Avoidance Banner
        if not hcp_summary.empty:
            _s = hcp_summary.iloc[0]
            ea_auto_vids = int(_s.get("vids_auto_resolved", 0))
            ea_total_vids = int(_s.get("unique_hcp_vids", 0))
            ea_saved = float(_s.get("effort_avoidance_hours", 0))
            ea_remaining = float(_s.get("est_remaining_review_hours", 0))
            ea_velocity = float(_s.get("manual_velocity_per_hr", 6.5))
            ea_pct = round(ea_auto_vids / ea_total_vids * 100, 1) if ea_total_vids else 0

            st.markdown(f"""
            <div class="effort-banner">
                <div>
                    <div class="effort-title">HCP Effort Avoidance</div>
                    <div style="color:rgba(167,243,208,0.4); font-size:0.7rem; margin-top:0.15rem;">@ {ea_velocity} VIDs/hr manual velocity</div>
                </div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value">{ea_auto_vids:,}</div><div class="effort-label">VIDs Auto-Resolved</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value">{ea_pct}%</div><div class="effort-label">Automation Rate</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value" style="color:#34d399;">{ea_saved:.1f} hrs</div><div class="effort-label">Manual Hours Saved</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value" style="color:#fbbf24;">{ea_remaining:.1f} hrs</div><div class="effort-label">Remaining Review</div></div>
            </div>
            """, unsafe_allow_html=True)

        # Charts Row
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        with chart_col1:
            st.markdown("##### Classification Breakdown")
            chart_data = pd.DataFrame({"Category": ["Auto-Merge", "Review", "Not Duplicate", "Unique"], "Count": [auto_ct, review_ct, notdup_ct, unique_ct]})
            if total_pairs > 0 or unique_ct > 0:
                import altair as alt
                pie = alt.Chart(chart_data).mark_arc(innerRadius=50, outerRadius=100).encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Category:N", scale=alt.Scale(domain=["Auto-Merge", "Review", "Not Duplicate", "Unique"], range=["#34d399", "#fbbf24", "#64748b", "#38bdf8"])),
                    tooltip=["Category", "Count"],
                ).properties(height=250).configure_view(strokeWidth=0)
                st.altair_chart(pie, use_container_width=True)
            else:
                st.info("No pairs to visualize")

        with chart_col2:
            st.markdown("##### Auto-Merge Rules")
            if not hcp_auto.empty and "rule" in hcp_auto.columns:
                rule_counts = hcp_auto["rule"].value_counts().reset_index()
                rule_counts.columns = ["Rule", "Count"]
                rule_descriptions = {"G1_NAME_SPL_HCO": "Name+Spec+HCO", "G2_NAME_SPL_PIN": "Name+Spec+PIN", "G3_NAME_SPL_CITY": "Name+Spec+City", "G3a_NAME_SPL_CITY_PHONE": "Name+Spec+City+Ph", "G3b_NAME_SPL_CITY_LICENSE": "Name+Spec+City+Lic", "G3c_NAME_SPL_CITY_UNCOMMON": "Name+Spec+City(Unc)", "G4_LICENSE": "License Match", "G5_PHONE_EMAIL": "Phone+Email", "G6_EMAIL_NAME": "Email+Name"}
                rule_counts["Label"] = rule_counts["Rule"].map(lambda x: rule_descriptions.get(x, x))
                import altair as alt
                bar = alt.Chart(rule_counts).mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6).encode(
                    x=alt.X("Count:Q", title="Pairs"), y=alt.Y("Label:N", sort="-x", title=""), color=alt.value("#6366f1"), tooltip=["Label", "Count"],
                ).properties(height=250).configure_view(strokeWidth=0)
                st.altair_chart(bar, use_container_width=True)
            else:
                st.info("No auto-merge pairs")

        with chart_col3:
            st.markdown("##### Name Similarity Distribution")
            all_sims = []
            for frame, label in [(hcp_auto, "Auto"), (hcp_review, "Review"), (hcp_notdup, "Not-Dup")]:
                if not frame.empty and "name_similarity" in frame.columns:
                    for s in frame["name_similarity"].dropna().astype(float):
                        all_sims.append({"Similarity": s, "Category": label})
            if all_sims:
                import altair as alt
                sim_df = pd.DataFrame(all_sims)
                hist = alt.Chart(sim_df).mark_bar(opacity=0.7).encode(
                    x=alt.X("Similarity:Q", bin=alt.Bin(maxbins=20), title="Name Similarity %"),
                    y=alt.Y("count()", title="Pairs"),
                    color=alt.Color("Category:N", scale=alt.Scale(domain=["Auto", "Review", "Not-Dup"], range=["#34d399", "#fbbf24", "#64748b"])),
                    tooltip=["Category", "count()"],
                ).properties(height=250).configure_view(strokeWidth=0)
                st.altair_chart(hist, use_container_width=True)
            else:
                st.info("No data for histogram")

        # Detailed Tabs
        st.markdown("---")
        st.markdown('<div class="section-header">Detailed Results</div>', unsafe_allow_html=True)

        tab_auto, tab_review, tab_notdup, tab_unique, tab_clusters, tab_shared = st.tabs([
            f"Auto-Merge ({auto_ct})", f"Review ({review_ct})", f"Not Duplicate ({notdup_ct})",
            f"Unique ({unique_ct})", f"Clusters ({cluster_ct})", f"Shared Contacts ({len(hcp_shared)})",
        ])

        display_cols_auto = ["vid_a", "vid_b", "rule", "name_similarity", "name_a", "name_b", "specialty_match", "cities_a", "cities_b", "comments", "rationale"]
        display_cols_review = ["vid_a", "vid_b", "score", "name_similarity", "name_a", "name_b", "specialty_match", "cities_a", "cities_b", "reasons", "comments", "rationale"]
        display_cols_notdup = ["vid_a", "vid_b", "reason", "name_similarity", "name_a", "name_b", "specialty_match", "cities_a", "cities_b", "comments", "rationale"]

        with tab_auto:
            if not hcp_auto.empty:
                rules = ["All"] + sorted(hcp_auto["rule"].unique().tolist()) if "rule" in hcp_auto.columns else ["All"]
                selected_rule = st.selectbox("Filter by rule", rules, key="auto_rule")
                filtered = hcp_auto if selected_rule == "All" else hcp_auto[hcp_auto["rule"] == selected_rule]
                st.dataframe(filtered[_filter_cols(filtered, display_cols_auto)], use_container_width=True, height=400)
            else:
                st.info("No auto-merge pairs found.")

        with tab_review:
            if not hcp_review.empty:
                min_score = int(hcp_review["score"].min()) if "score" in hcp_review.columns else 0
                max_score = int(hcp_review["score"].max()) if "score" in hcp_review.columns else 100
                score_range = st.slider("Score range", min_score, max_score, (min_score, max_score), key="review_score")
                filtered = hcp_review
                if "score" in hcp_review.columns:
                    filtered = hcp_review[(hcp_review["score"].astype(float) >= score_range[0]) & (hcp_review["score"].astype(float) <= score_range[1])]
                st.dataframe(filtered[_filter_cols(filtered, display_cols_review)], use_container_width=True, height=400)
            else:
                st.info("No review pairs found.")

        with tab_notdup:
            if not hcp_notdup.empty:
                st.dataframe(hcp_notdup[_filter_cols(hcp_notdup, display_cols_notdup)], use_container_width=True, height=400)
            else:
                st.info("No not-duplicate pairs.")

        with tab_unique:
            if not hcp_unique.empty:
                st.dataframe(hcp_unique, use_container_width=True, height=400)
            else:
                st.info("No unique VIDs — all records had at least one candidate pair.")

        with tab_clusters:
            if not hcp_clusters.empty:
                cluster_ids = sorted(hcp_clusters["cluster_id"].unique().tolist()) if "cluster_id" in hcp_clusters.columns else []
                if cluster_ids:
                    selected_cluster = st.selectbox("Select cluster", ["All"] + [str(c) for c in cluster_ids], key="cluster_select")
                    filtered = hcp_clusters if selected_cluster == "All" else hcp_clusters[hcp_clusters["cluster_id"].astype(str) == selected_cluster]
                    st.dataframe(filtered, use_container_width=True, height=400)
                else:
                    st.dataframe(hcp_clusters, use_container_width=True, height=400)
            else:
                st.info("No clusters found.")

        with tab_shared:
            if not hcp_shared.empty:
                st.dataframe(hcp_shared, use_container_width=True, height=400)
            else:
                st.info("No shared contacts detected at current threshold.")

        # Download Section
        st.markdown("---")
        st.markdown('<div class="section-header">Download Results</div>', unsafe_allow_html=True)
        dl_col1, dl_col2, dl_col3, dl_col4, dl_col5, dl_col6 = st.columns(6)
        with dl_col1:
            if not hcp_auto.empty:
                st.download_button("Download Auto-Merge", hcp_auto.to_csv(index=False), "HCP_AUTO_MERGE.csv", "text/csv", use_container_width=True)
        with dl_col2:
            if not hcp_review.empty:
                st.download_button("Download Review", hcp_review.to_csv(index=False), "HCP_REVIEW.csv", "text/csv", use_container_width=True)
        with dl_col3:
            if not hcp_notdup.empty:
                st.download_button("Download Not-Dup", hcp_notdup.to_csv(index=False), "HCP_NOT_DUP.csv", "text/csv", use_container_width=True)
        with dl_col4:
            if not hcp_unique.empty:
                st.download_button("Download Unique", hcp_unique.to_csv(index=False), "HCP_UNIQUE.csv", "text/csv", use_container_width=True)
        with dl_col5:
            hcp_tagged = build_tagged_source(results, entity_type="hcp")
            if not hcp_tagged.empty:
                st.download_button("Download Tagged Source", hcp_tagged.to_csv(index=False), "HCP_Tagged_Source.csv", "text/csv", use_container_width=True)
        with dl_col6:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as w:
                write_rules_sheet(w, _build_cfg(), entity_filter="hcp")
                if not hcp_summary.empty:
                    hcp_summary.to_excel(w, index=False, sheet_name="Summary")
                if not hcp_auto.empty:
                    hcp_auto.to_excel(w, index=False, sheet_name="AUTO_MERGE")
                    _format_vid_columns_as_text(w, "AUTO_MERGE", hcp_auto)
                if not hcp_review.empty:
                    hcp_review.to_excel(w, index=False, sheet_name="REVIEW")
                    _format_vid_columns_as_text(w, "REVIEW", hcp_review)
                if not hcp_notdup.empty:
                    hcp_notdup.to_excel(w, index=False, sheet_name="NOT_DUP")
                    _format_vid_columns_as_text(w, "NOT_DUP", hcp_notdup)
                if not hcp_unique.empty:
                    hcp_unique.to_excel(w, index=False, sheet_name="UNIQUE")
                    _format_vid_columns_as_text(w, "UNIQUE", hcp_unique)
                if not hcp_clusters.empty:
                    hcp_clusters.to_excel(w, index=False, sheet_name="CLUSTERS")
                    _format_vid_columns_as_text(w, "CLUSTERS", hcp_clusters)
                if not hcp_shared.empty:
                    hcp_shared.to_excel(w, index=False, sheet_name="Shared_Contacts")
                    _format_vid_columns_as_text(w, "Shared_Contacts", hcp_shared)
                if not hcp_tagged.empty:
                    hcp_tagged.to_excel(w, index=False, sheet_name="Tagged_Source")
                    _format_vid_columns_as_text(w, "Tagged_Source", hcp_tagged)
            st.download_button("Download Full Excel", buffer.getvalue(), "HCP_Dupe_Check.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    # ── HCO Results ───────────────────────────────────────────────────
    if st.session_state.hco_results is not None:
        hco_res = st.session_state.hco_results
        hco_auto = hco_res.get("hco_auto", pd.DataFrame())
        hco_review = hco_res.get("hco_review", pd.DataFrame())
        hco_notdup = hco_res.get("hco_notdup", pd.DataFrame())
        hco_unique = hco_res.get("hco_unique", pd.DataFrame())
        hco_clusters = hco_res.get("hco_clusters", pd.DataFrame())
        hco_summary = hco_res.get("hco_summary", pd.DataFrame())

        hco_auto_ct = len(hco_auto)
        hco_review_ct = len(hco_review)
        hco_notdup_ct = len(hco_notdup)
        hco_unique_ct = len(hco_unique)
        hco_cluster_ct = hco_clusters["cluster_id"].nunique() if not hco_clusters.empty and "cluster_id" in hco_clusters.columns else 0
        hco_total_pairs = hco_auto_ct + hco_review_ct + hco_notdup_ct

        st.markdown("---")
        st.markdown('<div class="section-header">HCO Results Overview</div>', unsafe_allow_html=True)

        hc1, hc2, hc3, hc4, hc5, hc6, hc7 = st.columns(7)
        with hc1:
            st.markdown(f'<div class="metric-card"><div class="metric-value vid-color">{hco_summary.iloc[0].get("unique_hco_vids", 0) if not hco_summary.empty else 0:,.0f}</div><div class="metric-label">Unique HCO VIDs</div></div>', unsafe_allow_html=True)
        with hc2:
            st.markdown(f'<div class="metric-card"><div class="metric-value pair-color">{hco_total_pairs:,}</div><div class="metric-label">Pairs Evaluated</div></div>', unsafe_allow_html=True)
        with hc3:
            st.markdown(f'<div class="metric-card"><div class="metric-value auto-color">{hco_auto_ct:,}</div><div class="metric-label">Auto-Merge</div></div>', unsafe_allow_html=True)
        with hc4:
            st.markdown(f'<div class="metric-card"><div class="metric-value review-color">{hco_review_ct:,}</div><div class="metric-label">Manual Review</div></div>', unsafe_allow_html=True)
        with hc5:
            st.markdown(f'<div class="metric-card"><div class="metric-value notdup-color">{hco_notdup_ct:,}</div><div class="metric-label">Not Duplicate</div></div>', unsafe_allow_html=True)
        with hc6:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#38bdf8;">{hco_unique_ct:,}</div><div class="metric-label">Unique (No Pairs)</div></div>', unsafe_allow_html=True)
        with hc7:
            st.markdown(f'<div class="metric-card"><div class="metric-value cluster-color">{hco_cluster_ct:,}</div><div class="metric-label">Clusters</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # HCO Effort Avoidance Banner
        if not hco_summary.empty:
            _hs = hco_summary.iloc[0]
            hco_ea_auto_vids = int(_hs.get("vids_auto_resolved", 0))
            hco_ea_total_vids = int(_hs.get("unique_hco_vids", 0))
            hco_ea_saved = float(_hs.get("effort_avoidance_hours", 0))
            hco_ea_remaining = float(_hs.get("est_remaining_review_hours", 0))
            hco_ea_velocity = float(_hs.get("manual_velocity_per_hr", 6.5))
            hco_ea_pct = round(hco_ea_auto_vids / hco_ea_total_vids * 100, 1) if hco_ea_total_vids else 0

            st.markdown(f"""
            <div class="effort-banner">
                <div><div class="effort-title">HCO Effort Avoidance</div><div style="color:rgba(167,243,208,0.4); font-size:0.7rem; margin-top:0.15rem;">@ {hco_ea_velocity} VIDs/hr manual velocity</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value">{hco_ea_auto_vids:,}</div><div class="effort-label">VIDs Auto-Resolved</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value">{hco_ea_pct}%</div><div class="effort-label">Automation Rate</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value" style="color:#34d399;">{hco_ea_saved:.1f} hrs</div><div class="effort-label">Manual Hours Saved</div></div>
                <div class="effort-divider"></div>
                <div class="effort-stat"><div class="effort-value" style="color:#fbbf24;">{hco_ea_remaining:.1f} hrs</div><div class="effort-label">Remaining Review</div></div>
            </div>
            """, unsafe_allow_html=True)

        # HCO Charts
        import altair as alt
        hco_chart1, hco_chart2 = st.columns(2)
        with hco_chart1:
            st.markdown("##### HCO Classification Breakdown")
            hco_chart_data = pd.DataFrame({"Category": ["Auto-Merge", "Review", "Not Duplicate", "Unique"], "Count": [hco_auto_ct, hco_review_ct, hco_notdup_ct, hco_unique_ct]})
            if hco_total_pairs > 0 or hco_unique_ct > 0:
                hco_pie = alt.Chart(hco_chart_data).mark_arc(innerRadius=50, outerRadius=100).encode(theta=alt.Theta("Count:Q"), color=alt.Color("Category:N", scale=alt.Scale(domain=["Auto-Merge", "Review", "Not Duplicate", "Unique"], range=["#34d399", "#fbbf24", "#64748b", "#38bdf8"])), tooltip=["Category", "Count"]).properties(height=250).configure_view(strokeWidth=0)
                st.altair_chart(hco_pie, use_container_width=True)
            else:
                st.info("No HCO pairs to visualize")
        with hco_chart2:
            st.markdown("##### HCO Auto-Merge Rules")
            if not hco_auto.empty and "rule" in hco_auto.columns:
                hco_rule_counts = hco_auto["rule"].value_counts().reset_index()
                hco_rule_counts.columns = ["Rule", "Count"]
                hco_rule_descs = {"H1_NAME_ADDR_PHONE": "Name+Addr+Phone", "H2_NAME_ADDR_TYPE": "Name+Addr+Type", "H3_NAME_PHONE_TYPE": "Name+Phone+Type"}
                hco_rule_counts["Label"] = hco_rule_counts["Rule"].map(lambda x: hco_rule_descs.get(x, x))
                hco_bar = alt.Chart(hco_rule_counts).mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6).encode(x=alt.X("Count:Q", title="Pairs"), y=alt.Y("Label:N", sort="-x", title=""), color=alt.value("#a78bfa"), tooltip=["Label", "Count"]).properties(height=250).configure_view(strokeWidth=0)
                st.altair_chart(hco_bar, use_container_width=True)
            else:
                st.info("No HCO auto-merge pairs")

        # HCO Detailed Tabs
        st.markdown("---")
        st.markdown('<div class="section-header">HCO Detailed Results</div>', unsafe_allow_html=True)
        hco_tab_auto, hco_tab_review, hco_tab_notdup, hco_tab_unique, hco_tab_clusters = st.tabs([
            f"Auto-Merge ({hco_auto_ct})", f"Review ({hco_review_ct})", f"Not Duplicate ({hco_notdup_ct})",
            f"Unique ({hco_unique_ct})", f"Clusters ({hco_cluster_ct})",
        ])
        hco_display_auto = ["vid_a", "vid_b", "rule", "name_similarity", "name_a", "name_b", "type_a", "type_b", "city_a", "city_b", "comments"]
        hco_display_review = ["vid_a", "vid_b", "score", "name_similarity", "name_a", "name_b", "type_a", "type_b", "city_a", "city_b", "reasons", "comments"]
        hco_display_notdup = ["vid_a", "vid_b", "reason", "name_similarity", "name_a", "name_b", "type_a", "type_b", "city_a", "city_b", "comments"]

        with hco_tab_auto:
            if not hco_auto.empty:
                hco_rules_list = ["All"] + sorted(hco_auto["rule"].unique().tolist()) if "rule" in hco_auto.columns else ["All"]
                hco_selected_rule = st.selectbox("Filter by rule", hco_rules_list, key="hco_auto_rule")
                hco_filtered = hco_auto if hco_selected_rule == "All" else hco_auto[hco_auto["rule"] == hco_selected_rule]
                st.dataframe(hco_filtered[_filter_cols(hco_filtered, hco_display_auto)], use_container_width=True, height=400)
            else:
                st.info("No HCO auto-merge pairs found.")
        with hco_tab_review:
            if not hco_review.empty:
                hco_min_score = int(hco_review["score"].min()) if "score" in hco_review.columns else 0
                hco_max_score = int(hco_review["score"].max()) if "score" in hco_review.columns else 100
                hco_score_range = st.slider("Score range", hco_min_score, hco_max_score, (hco_min_score, hco_max_score), key="hco_review_score")
                hco_rev_filtered = hco_review
                if "score" in hco_review.columns:
                    hco_rev_filtered = hco_review[(hco_review["score"].astype(float) >= hco_score_range[0]) & (hco_review["score"].astype(float) <= hco_score_range[1])]
                st.dataframe(hco_rev_filtered[_filter_cols(hco_rev_filtered, hco_display_review)], use_container_width=True, height=400)
            else:
                st.info("No HCO review pairs found.")
        with hco_tab_notdup:
            if not hco_notdup.empty:
                st.dataframe(hco_notdup[_filter_cols(hco_notdup, hco_display_notdup)], use_container_width=True, height=400)
            else:
                st.info("No HCO not-duplicate pairs.")
        with hco_tab_unique:
            if not hco_unique.empty:
                st.dataframe(hco_unique, use_container_width=True, height=400)
            else:
                st.info("No unique VIDs — all records had at least one candidate pair.")

        with hco_tab_clusters:
            if not hco_clusters.empty:
                hco_cluster_ids = sorted(hco_clusters["cluster_id"].unique().tolist()) if "cluster_id" in hco_clusters.columns else []
                if hco_cluster_ids:
                    hco_sel_cluster = st.selectbox("Select cluster", ["All"] + [str(c) for c in hco_cluster_ids], key="hco_cluster_select")
                    hco_cl_filtered = hco_clusters if hco_sel_cluster == "All" else hco_clusters[hco_clusters["cluster_id"].astype(str) == hco_sel_cluster]
                    st.dataframe(hco_cl_filtered, use_container_width=True, height=400)
                else:
                    st.dataframe(hco_clusters, use_container_width=True, height=400)
            else:
                st.info("No HCO clusters found.")

        # HCO Download
        st.markdown("---")
        st.markdown('<div class="section-header">HCO Download Results</div>', unsafe_allow_html=True)
        hco_dl1, hco_dl2, hco_dl3, hco_dl4, hco_dl5, hco_dl6 = st.columns(6)
        with hco_dl1:
            if not hco_auto.empty:
                st.download_button("Download HCO Auto-Merge", hco_auto.to_csv(index=False), "HCO_AUTO_MERGE.csv", "text/csv", use_container_width=True)
        with hco_dl2:
            if not hco_review.empty:
                st.download_button("Download HCO Review", hco_review.to_csv(index=False), "HCO_REVIEW.csv", "text/csv", use_container_width=True)
        with hco_dl3:
            if not hco_notdup.empty:
                st.download_button("Download HCO Not-Dup", hco_notdup.to_csv(index=False), "HCO_NOT_DUP.csv", "text/csv", use_container_width=True)
        with hco_dl4:
            if not hco_unique.empty:
                st.download_button("Download HCO Unique", hco_unique.to_csv(index=False), "HCO_UNIQUE.csv", "text/csv", use_container_width=True)
        with hco_dl5:
            hco_tagged = build_tagged_source(hco_res, entity_type="hco")
            if not hco_tagged.empty:
                st.download_button("Download Tagged Source", hco_tagged.to_csv(index=False), "HCO_Tagged_Source.csv", "text/csv", use_container_width=True)
        with hco_dl6:
            hco_buffer = io.BytesIO()
            with pd.ExcelWriter(hco_buffer, engine="openpyxl") as w:
                write_rules_sheet(w, _build_cfg(), entity_filter="hco")
                if not hco_summary.empty:
                    hco_summary.to_excel(w, index=False, sheet_name="Summary")
                if not hco_auto.empty:
                    hco_auto.to_excel(w, index=False, sheet_name="AUTO_MERGE")
                    _format_vid_columns_as_text(w, "AUTO_MERGE", hco_auto)
                if not hco_review.empty:
                    hco_review.to_excel(w, index=False, sheet_name="REVIEW")
                    _format_vid_columns_as_text(w, "REVIEW", hco_review)
                if not hco_notdup.empty:
                    hco_notdup.to_excel(w, index=False, sheet_name="NOT_DUP")
                    _format_vid_columns_as_text(w, "NOT_DUP", hco_notdup)
                if not hco_unique.empty:
                    hco_unique.to_excel(w, index=False, sheet_name="UNIQUE")
                    _format_vid_columns_as_text(w, "UNIQUE", hco_unique)
                if not hco_clusters.empty:
                    hco_clusters.to_excel(w, index=False, sheet_name="CLUSTERS")
                    _format_vid_columns_as_text(w, "CLUSTERS", hco_clusters)
                if not hco_tagged.empty:
                    hco_tagged.to_excel(w, index=False, sheet_name="Tagged_Source")
                    _format_vid_columns_as_text(w, "Tagged_Source", hco_tagged)
            st.download_button("Download HCO Full Excel", hco_buffer.getvalue(), "HCO_Dupe_Check.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    # Landing page (no data, no results)
    if not _has_any_results and not has_data:
        st.markdown("<br>", unsafe_allow_html=True)
        landing_hcp, landing_hco = st.tabs(["HCP Pipeline", "HCO Pipeline"])
        with landing_hcp:
            st.markdown("#### Healthcare Professional Duplicate Detection")
            st.markdown("Upload a Veeva extract containing **`hcp.vid__v (VID)`** and the tool will automatically detect and classify duplicate HCP records.")
            f1, f2, f3 = st.columns(3)
            with f1:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x1F4CA;</div><div class="feature-title">Smart Matching</div><div class="feature-desc">Name + Specialty + Affiliation<br>Fuzzy matching catches spelling variants<br>License conflict detection</div></div>', unsafe_allow_html=True)
            with f2:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x26A1;</div><div class="feature-title">Tiered Classification</div><div class="feature-desc">Auto-Merge: High confidence pairs<br>Review: Needs human judgment<br>Not-Dup: Safely excluded</div></div>', unsafe_allow_html=True)
            with f3:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x1F512;</div><div class="feature-title">Shared Contact Aware</div><div class="feature-desc">Detects hospital switchboard numbers<br>Prevents false positives<br>Configurable thresholds</div></div>', unsafe_allow_html=True)
        with landing_hco:
            st.markdown("#### Healthcare Organization Duplicate Detection")
            st.markdown("Upload a Veeva extract containing **`hco.vid__v (VID)`** and the tool will automatically detect and classify duplicate HCO records.")
            h1, h2, h3 = st.columns(3)
            with h1:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x1F3E5;</div><div class="feature-title">Name + Address</div><div class="feature-desc">Fuzzy name matching<br>Address line token overlap<br>Postal code + City verification</div></div>', unsafe_allow_html=True)
            with h2:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x1F4DE;</div><div class="feature-title">Phone & Type</div><div class="feature-desc">Phone / Fax overlap detection<br>HCO type consistency check<br>Multi-signal corroboration</div></div>', unsafe_allow_html=True)
            with h3:
                st.markdown('<div class="feature-card"><div class="feature-icon">&#x26A1;</div><div class="feature-title">Same 3-Tier Engine</div><div class="feature-desc">Auto-Merge: High confidence pairs<br>Review: Needs human judgment<br>Not-Dup: Safely excluded</div></div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  MODE 2: CHECK BEFORE CREATE (DS single-record lookup)
# ══════════════════════════════════════════════════════════════════════

with mode_tab_lookup:

    st.markdown("""
    <div style="background:rgba(30,41,59,0.5); border:1px solid rgba(99,102,241,0.15); border-radius:14px; padding:1.5rem; margin-bottom:1.5rem; backdrop-filter:blur(10px);">
        <div style="color:#a5b4fc; font-size:1.1rem; font-weight:600; margin-bottom:0.4rem;">Check Before Create</div>
        <div style="color:rgba(148,163,184,0.7); font-size:0.85rem;">
            Before creating a new HCP record, check if a matching profile already exists in the database.
            <br><b>Step 1:</b> Load your existing DB extract (reference database).
            <b>Step 2:</b> Enter the HCP details and search.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Step 1: Load Reference DB
    st.markdown("##### Step 1: Load Reference Database")

    ref_input_method = st.radio(
        "How do you want to load the reference DB?",
        ["Upload file", "Enter file path"],
        horizontal=True,
        key="ref_input_method",
    )

    ref_uploaded = None
    ref_path = None

    if ref_input_method == "Upload file":
        ref_uploaded = st.file_uploader(
            "Upload your existing DB extract (Veeva VOD format)",
            type=["csv", "xlsx", "xls"],
            key="ref_uploader",
        )
    else:
        ref_path = st.text_input(
            "Full path to your reference DB file",
            placeholder="/Users/you/Desktop/data/existing_db.csv",
            key="ref_path",
        )
        if ref_path and not os.path.isfile(ref_path):
            st.warning(f"File not found: `{ref_path}`")
            ref_path = None

    ref_has_data = ref_uploaded is not None or (ref_path is not None and ref_path)

    # Build or show existing index
    if ref_has_data or st.session_state.ref_index is not None:
        if st.session_state.ref_index is None:
            build_clicked = st.button("Build Reference Index", type="primary", key="build_ref_index")
            if build_clicked:
                with st.spinner("Loading reference data..."):
                    if ref_uploaded is not None:
                        ref_df = load_data_upload(ref_uploaded.getvalue(), ref_uploaded.name)
                    else:
                        ref_df = load_data_path(ref_path)

                cfg = _build_cfg()

                # Validate that the required VID column exists
                vid_col = cfg["columns"]["hcp_vid"]
                if vid_col not in ref_df.columns:
                    # Try common alternate names
                    _vid_aliases = [
                        c for c in ref_df.columns
                        if "vid" in c.lower() and "hco" not in c.lower()
                        and "parent" not in c.lower() and "grandparent" not in c.lower()
                    ]
                    if len(_vid_aliases) == 1:
                        cfg["columns"]["hcp_vid"] = _vid_aliases[0]
                        st.info(f"Auto-mapped VID column: `{_vid_aliases[0]}`")
                    else:
                        st.error(
                            f"Required column `{vid_col}` not found in reference file.\n\n"
                            f"**Your columns:** {', '.join(ref_df.columns[:20])}"
                            + (f" ... (+{len(ref_df.columns)-20} more)" if len(ref_df.columns) > 20 else "")
                            + "\n\nPlease upload a Veeva extract with the standard column headers."
                        )
                        st.stop()

                # Also check last_name column
                ln_col = cfg["columns"]["hcp_last_name"]
                if ln_col not in ref_df.columns:
                    _ln_aliases = [
                        c for c in ref_df.columns
                        if any(kw in c.lower() for kw in ["last_name", "last name", "surname", "family_name"])
                    ]
                    if len(_ln_aliases) >= 1:
                        cfg["columns"]["hcp_last_name"] = _ln_aliases[0]
                        st.info(f"Auto-mapped Last Name column: `{_ln_aliases[0]}`")
                    else:
                        st.error(
                            f"Required column `{ln_col}` not found in reference file.\n\n"
                            f"**Your columns:** {', '.join(ref_df.columns[:20])}"
                            + (f" ... (+{len(ref_df.columns)-20} more)" if len(ref_df.columns) > 20 else "")
                            + "\n\nPlease upload a Veeva extract with the standard column headers."
                        )
                        st.stop()

                progress_placeholder = st.empty()

                def _ref_progress(msg):
                    progress_placeholder.info(msg)

                ref_index = build_reference_index(ref_df, cfg, progress_fn=_ref_progress)
                st.session_state.ref_index = ref_index

                # Persist to disk for fast reload after browser refresh
                _save_ref_index(ref_index)

                progress_placeholder.empty()
                st.rerun()
        else:
            ref_index = st.session_state.ref_index
            cache_age = _ref_cache_age()
            age_str = f" (cached {cache_age})" if cache_age else ""
            st.success(f"Reference DB loaded: **{ref_index.vid_count:,}** unique HCP VIDs indexed.{age_str}")

            clear_col, reload_col = st.columns(2)
            with clear_col:
                if st.button("Clear Reference Index", key="clear_ref"):
                    st.session_state.ref_index = None
                    st.session_state.lookup_results = None
                    _clear_ref_cache()
                    st.rerun()
            with reload_col:
                if ref_has_data and st.button("Reload Reference DB", key="reload_ref",
                                              help="Rebuild from source file (use when the source DB has been updated)"):
                    st.session_state.ref_index = None
                    st.session_state.lookup_results = None
                    _clear_ref_cache()
                    st.rerun()

    # Step 2: HCP Lookup Form
    if st.session_state.ref_index is not None:
        st.markdown("---")
        st.markdown("##### Step 2: Enter HCP Details to Search")

        col_form1, col_form2 = st.columns(2)

        with col_form1:
            lookup_last = st.text_input("Last Name *", key="lookup_last", placeholder="e.g. Sharma")
            lookup_first = st.text_input("First Name", key="lookup_first", placeholder="e.g. Rajesh")
            lookup_specialty = st.text_input("Specialty", key="lookup_spec", placeholder="e.g. Cardiology")
            lookup_city = st.text_input("City", key="lookup_city", placeholder="e.g. Mumbai")

        with col_form2:
            lookup_phone = st.text_input("Phone", key="lookup_phone", placeholder="e.g. 9876543210")
            lookup_email = st.text_input("Email", key="lookup_email", placeholder="e.g. dr.sharma@hospital.com")
            lookup_license = st.text_input("License Number", key="lookup_lic", placeholder="e.g. MH12345")
            lookup_lic_body = st.text_input("Licensing Body", key="lookup_lic_body", placeholder="e.g. Maharashtra Medical Council")

        if not lookup_last:
            st.caption("Enter at least a **Last Name** to search.")
        else:
            search_clicked = st.button("Search for Matches", type="primary", use_container_width=True, key="lookup_search")

            if search_clicked:
                form = {
                    "first_name": lookup_first,
                    "last_name": lookup_last,
                    "specialty": lookup_specialty,
                    "city": lookup_city,
                    "phone": lookup_phone,
                    "email": lookup_email,
                    "license_number": lookup_license,
                    "license_body": lookup_lic_body,
                }

                t0 = time.time()
                matches = lookup_single(form, st.session_state.ref_index)
                elapsed = time.time() - t0

                st.session_state.lookup_results = {
                    "matches": matches,
                    "elapsed": elapsed,
                    "query": f"{lookup_first} {lookup_last}".strip(),
                }
                st.rerun()

        # Display results
        if st.session_state.lookup_results is not None:
            lr = st.session_state.lookup_results
            matches = lr["matches"]
            elapsed = lr["elapsed"]
            query_name = lr["query"]

            st.markdown("---")

            if not matches:
                st.markdown(f"""
                <div class="verdict-clean">
                    <div style="font-size:1.5rem; font-weight:700; color:#34d399; margin-bottom:0.3rem;">No Matches Found</div>
                    <div style="color:rgba(167,243,208,0.7); font-size:0.9rem;">
                        No existing profile matches "<b>{query_name}</b>" in the database ({st.session_state.ref_index.vid_count:,} records searched in {elapsed:.2f}s).
                        <br>It is safe to create a new record.
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                dup_count = sum(1 for m in matches if m["verdict"] == "LIKELY_DUPLICATE")
                poss_count = sum(1 for m in matches if m["verdict"] == "POSSIBLE_MATCH")

                if dup_count > 0:
                    st.markdown(f"""
                    <div class="verdict-high">
                        <div style="color:#ef4444; font-size:1.1rem; font-weight:700;">STOP: {dup_count} Likely Duplicate(s) Found</div>
                        <div style="color:rgba(252,165,165,0.7); font-size:0.82rem; margin-top:0.3rem;">
                            Do NOT create a new record. "{query_name}" likely already exists. ({elapsed:.2f}s, {st.session_state.ref_index.vid_count:,} records searched)
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                elif poss_count > 0:
                    st.markdown(f"""
                    <div class="verdict-medium">
                        <div style="color:#fbbf24; font-size:1.1rem; font-weight:700;">REVIEW: {poss_count} Possible Match(es) Found</div>
                        <div style="color:rgba(253,230,138,0.7); font-size:0.82rem; margin-top:0.3rem;">
                            Review the matches below before creating a new record for "{query_name}". ({elapsed:.2f}s)
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                # Match details table
                match_df = pd.DataFrame(matches)
                display_cols = [
                    "verdict", "confidence", "score", "vid", "name",
                    "name_similarity", "specialties", "cities",
                    "phones", "emails", "rule", "rationale",
                ]
                display_cols = [c for c in display_cols if c in match_df.columns]
                st.dataframe(match_df[display_cols], use_container_width=True, height=min(400, 80 + 35 * len(matches)))

                # Download
                csv_data = match_df.to_csv(index=False)
                st.download_button(
                    f"Download {len(matches)} Match(es)",
                    csv_data,
                    f"lookup_{query_name.replace(' ', '_')}.csv",
                    "text/csv",
                    key="dl_lookup",
                )


# ══════════════════════════════════════════════════════════════════════
#  MODE 3: PDR PRE-SCREEN (batch new records vs existing DB)
# ══════════════════════════════════════════════════════════════════════

with mode_tab_pdr:

    st.markdown("""
    <div style="background:rgba(30,41,59,0.5); border:1px solid rgba(99,102,241,0.15); border-radius:14px; padding:1.5rem; margin-bottom:1.5rem; backdrop-filter:blur(10px);">
        <div style="color:#a5b4fc; font-size:1.1rem; font-weight:600; margin-bottom:0.4rem;">PDR Pre-Screening</div>
        <div style="color:rgba(148,163,184,0.7); font-size:0.85rem;">
            Screen a batch of new PDR records against your existing database before processing.
            <br>Prevents duplicate creation and wasted effort.
            <br><b>Step 1:</b> Load your existing DB extract.
            <b>Step 2:</b> Upload the PDR batch.
            <b>Step 3:</b> Review results and download.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Step 1: Reference DB (reuses same index as Check Before Create)
    st.markdown("##### Step 1: Load Reference Database")

    if st.session_state.ref_index is not None:
        st.success(f"Reference DB loaded: **{st.session_state.ref_index.vid_count:,}** unique HCP VIDs indexed. (Shared with 'Check Before Create' tab)")
    else:
        st.info("Load your reference database in the **Check Before Create** tab first, then return here.")

    # Step 2: Upload PDR batch
    if st.session_state.ref_index is not None:
        st.markdown("---")
        st.markdown("##### Step 2: Upload PDR Batch")

        pdr_input_method = st.radio(
            "How do you want to load the PDR batch?",
            ["Upload file", "Enter file path"],
            horizontal=True,
            key="pdr_input_method",
        )

        pdr_uploaded = None
        pdr_path = None

        if pdr_input_method == "Upload file":
            pdr_uploaded = st.file_uploader(
                "Upload your PDR batch (same Veeva column format)",
                type=["csv", "xlsx", "xls"],
                key="pdr_uploader",
            )
        else:
            pdr_path = st.text_input(
                "Full path to your PDR batch file",
                placeholder="/Users/you/Desktop/data/pdr_batch.csv",
                key="pdr_path",
            )
            if pdr_path and not os.path.isfile(pdr_path):
                st.warning(f"File not found: `{pdr_path}`")
                pdr_path = None

        pdr_has_data = pdr_uploaded is not None or (pdr_path is not None and pdr_path)

        if pdr_has_data:
            with st.spinner("Loading PDR batch..."):
                if pdr_uploaded is not None:
                    pdr_df = load_data_upload(pdr_uploaded.getvalue(), pdr_uploaded.name)
                else:
                    pdr_df = load_data_path(pdr_path)

            hcp_vid_col = "hcp.vid__v (VID)"
            if hcp_vid_col not in pdr_df.columns:
                # Try common aliases (e.g. Veeva exports use "NETWORK ID")
                _pdr_vid_aliases = [
                    c for c in pdr_df.columns
                    if c.lower().startswith("hcp.vid__v")
                ]
                if len(_pdr_vid_aliases) == 1:
                    pdr_df = pdr_df.rename(columns={_pdr_vid_aliases[0]: hcp_vid_col})
                    st.info(f"Auto-mapped PDR VID column: `{_pdr_vid_aliases[0]}` → `{hcp_vid_col}`")
                else:
                    st.error(f"PDR batch must contain `{hcp_vid_col}` column.")
            if hcp_vid_col in pdr_df.columns:
                pdr_vids = pdr_df[hcp_vid_col].nunique()
                st.info(f"PDR batch: **{len(pdr_df):,}** rows, **{pdr_vids:,}** unique VIDs")

                screen_clicked = st.button("Screen PDR Batch", type="primary", use_container_width=True, key="pdr_screen")

                if screen_clicked:
                    cfg = _build_cfg()
                    t0 = time.time()
                    progress_bar = st.progress(0, text="Starting PDR screening...")
                    status_placeholder = st.empty()

                    def _pdr_progress(pct, msg):
                        progress_bar.progress(min(pct, 99), text=msg)
                        status_placeholder.caption(msg)

                    pdr_result = screen_pdr_batch(pdr_df, st.session_state.ref_index, progress_fn=_pdr_progress)
                    elapsed = time.time() - t0
                    progress_bar.progress(100, text="Done!")
                    status_placeholder.empty()

                    st.session_state.pdr_results = {
                        "result_df": pdr_result,
                        "elapsed": elapsed,
                        "total_pdr": pdr_vids,
                    }
                    st.rerun()

        # Display PDR results
        if st.session_state.pdr_results is not None:
            st.markdown("---")
            pdr_res = st.session_state.pdr_results
            pdr_result_df = pdr_res["result_df"]
            pdr_elapsed = pdr_res["elapsed"]
            pdr_total = pdr_res["total_pdr"]

            if pdr_result_df.empty:
                st.info("No PDR records to screen.")
            else:
                dup_ct = len(pdr_result_df[pdr_result_df["pdr_verdict"] == "LIKELY_DUP"])
                poss_ct = len(pdr_result_df[pdr_result_df["pdr_verdict"] == "POSSIBLE_MATCH"])
                clean_ct = len(pdr_result_df[pdr_result_df["pdr_verdict"] == "CLEAN"])

                st.markdown(f'<div class="section-header">PDR Screening Results ({pdr_elapsed:.1f}s)</div>', unsafe_allow_html=True)

                # KPI cards
                kc1, kc2, kc3, kc4 = st.columns(4)
                with kc1:
                    st.markdown(f'<div class="metric-card"><div class="metric-value vid-color">{pdr_total:,}</div><div class="metric-label">PDR Records</div></div>', unsafe_allow_html=True)
                with kc2:
                    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#ef4444;">{dup_ct:,}</div><div class="metric-label">Likely Duplicates</div></div>', unsafe_allow_html=True)
                with kc3:
                    st.markdown(f'<div class="metric-card"><div class="metric-value review-color">{poss_ct:,}</div><div class="metric-label">Possible Matches</div></div>', unsafe_allow_html=True)
                with kc4:
                    st.markdown(f'<div class="metric-card"><div class="metric-value auto-color">{clean_ct:,}</div><div class="metric-label">Clean (Safe)</div></div>', unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                # Effort avoidance
                dupes_prevented = dup_ct + poss_ct
                if dupes_prevented > 0:
                    st.markdown(f"""
                    <div class="effort-banner">
                        <div><div class="effort-title">PDR Dupe Prevention</div></div>
                        <div class="effort-divider"></div>
                        <div class="effort-stat"><div class="effort-value" style="color:#ef4444;">{dup_ct:,}</div><div class="effort-label">Dupes Blocked</div></div>
                        <div class="effort-divider"></div>
                        <div class="effort-stat"><div class="effort-value" style="color:#fbbf24;">{poss_ct:,}</div><div class="effort-label">Need Review</div></div>
                        <div class="effort-divider"></div>
                        <div class="effort-stat"><div class="effort-value" style="color:#34d399;">{clean_ct:,}</div><div class="effort-label">Safe to Process</div></div>
                        <div class="effort-divider"></div>
                        <div class="effort-stat"><div class="effort-value">{round(dupes_prevented / len(pdr_result_df) * 100, 1) if len(pdr_result_df) else 0}%</div><div class="effort-label">Flagged Rate</div></div>
                    </div>
                    """, unsafe_allow_html=True)

                # Filter tabs
                pdr_tab_all, pdr_tab_dup, pdr_tab_poss, pdr_tab_clean = st.tabs([
                    f"All ({len(pdr_result_df)})",
                    f"Likely Duplicates ({dup_ct})",
                    f"Possible Matches ({poss_ct})",
                    f"Clean ({clean_ct})",
                ])

                pdr_display_cols = [
                    "pdr_vid", "pdr_name", "pdr_specialties", "pdr_cities",
                    "pdr_verdict", "match_count", "best_match_vid",
                    "best_match_name", "best_match_score", "best_match_name_sim",
                    "best_match_rule", "best_match_rationale",
                ]
                pdr_display = [c for c in pdr_display_cols if c in pdr_result_df.columns]

                with pdr_tab_all:
                    st.dataframe(pdr_result_df[pdr_display], use_container_width=True, height=500)
                with pdr_tab_dup:
                    dup_df = pdr_result_df[pdr_result_df["pdr_verdict"] == "LIKELY_DUP"]
                    if not dup_df.empty:
                        st.dataframe(dup_df[pdr_display], use_container_width=True, height=400)
                    else:
                        st.info("No likely duplicates found.")
                with pdr_tab_poss:
                    poss_df = pdr_result_df[pdr_result_df["pdr_verdict"] == "POSSIBLE_MATCH"]
                    if not poss_df.empty:
                        st.dataframe(poss_df[pdr_display], use_container_width=True, height=400)
                    else:
                        st.info("No possible matches found.")
                with pdr_tab_clean:
                    clean_df = pdr_result_df[pdr_result_df["pdr_verdict"] == "CLEAN"]
                    if not clean_df.empty:
                        st.dataframe(clean_df[pdr_display], use_container_width=True, height=400)
                    else:
                        st.info("No clean records (all have matches).")

                # Download
                st.markdown("---")
                st.markdown('<div class="section-header">Download PDR Results</div>', unsafe_allow_html=True)
                pdr_dl1, pdr_dl2, pdr_dl3 = st.columns(3)
                with pdr_dl1:
                    st.download_button("Download All Results", pdr_result_df.to_csv(index=False), "PDR_PreScreen_All.csv", "text/csv", use_container_width=True)
                with pdr_dl2:
                    flagged = pdr_result_df[pdr_result_df["pdr_verdict"] != "CLEAN"]
                    if not flagged.empty:
                        st.download_button("Download Flagged Only", flagged.to_csv(index=False), "PDR_PreScreen_Flagged.csv", "text/csv", use_container_width=True)
                with pdr_dl3:
                    pdr_buffer = io.BytesIO()
                    with pd.ExcelWriter(pdr_buffer, engine="openpyxl") as w:
                        write_rules_sheet(w, _build_cfg(), entity_filter="hcp")
                        summary_data = pd.DataFrame([{
                            "total_pdr_records": len(pdr_result_df),
                            "likely_duplicates": dup_ct,
                            "possible_matches": poss_ct,
                            "clean_records": clean_ct,
                            "reference_db_vids": st.session_state.ref_index.vid_count,
                            "screening_time_seconds": round(pdr_elapsed, 1),
                        }])
                        summary_data.to_excel(w, index=False, sheet_name="Summary")
                        pdr_result_df.to_excel(w, index=False, sheet_name="All_Results")
                        if dup_ct > 0:
                            pdr_result_df[pdr_result_df["pdr_verdict"] == "LIKELY_DUP"].to_excel(w, index=False, sheet_name="Likely_Duplicates")
                        if poss_ct > 0:
                            pdr_result_df[pdr_result_df["pdr_verdict"] == "POSSIBLE_MATCH"].to_excel(w, index=False, sheet_name="Possible_Matches")
                        if clean_ct > 0:
                            pdr_result_df[pdr_result_df["pdr_verdict"] == "CLEAN"].to_excel(w, index=False, sheet_name="Clean")
                    st.download_button("Download Full Excel", pdr_buffer.getvalue(), "PDR_PreScreen.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)


# ══════════════════════════════════════════════════════════════════════
#  MODE 4: CROSS-DB MATCH
# ══════════════════════════════════════════════════════════════════════

with mode_tab_xmatch:
    st.markdown("""
    <div style="background:rgba(30,41,59,0.5); border:1px solid rgba(99,102,241,0.15);
         border-radius:14px; padding:1.5rem; margin-bottom:1.5rem; backdrop-filter:blur(10px);">
        <div style="color:#a5b4fc; font-size:1.1rem; font-weight:600; margin-bottom:0.4rem;">
            Cross-DB Match</div>
        <div style="color:rgba(148,163,184,0.7); font-size:0.85rem;">
            Match records from any HCP source file (Veeva Link KOL exports, custom lists, etc.)
            against your master Veeva OpenData database.
            <br>The tool auto-detects column headers and maps data fields automatically.
            <br><b>Step 1:</b> Load your master DB (same index as Check Before Create / PDR tabs).
            <b>Step 2:</b> Upload source file(s).
            <b>Step 3:</b> Review matches and download results.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Step 1: Master DB ────────────────────────────────────────────
    st.markdown('<div class="section-header">Step 1 &mdash; Master Database</div>', unsafe_allow_html=True)

    if st.session_state.ref_index is not None:
        st.success(
            f"Master DB indexed: **{st.session_state.ref_index.vid_count:,}** unique HCP VIDs ready."
        )
    else:
        st.info(
            "No master index found. Load your master database in the "
            "**Check Before Create** tab first, then return here."
        )

    # ── Step 2: Upload source file ───────────────────────────────────
    if st.session_state.ref_index is not None:
        st.markdown("---")
        st.markdown('<div class="section-header">Step 2 &mdash; Upload Source File</div>', unsafe_allow_html=True)

        xm_source = st.file_uploader(
            "Upload your KOL / source HCP file (any Excel or CSV with HCP data)",
            type=["csv", "xlsx", "xls"],
            key="xmatch_uploader",
        )

        if xm_source is not None:
            # Auto-detect header and columns
            with st.spinner("Detecting headers and mapping columns..."):
                xm_src_df, xm_header_row = detect_header_row(
                    xm_source.getvalue(), xm_source.name,
                )
                xm_col_map = auto_detect_columns(xm_src_df)

            st.info(
                f"Header detected at row **{xm_header_row + 1}**. "
                f"Loaded **{len(xm_src_df):,}** records with "
                f"**{len(xm_src_df.columns)}** columns."
            )

            # Show detected mapping
            with st.expander("Column Mapping (auto-detected)", expanded=False):
                map_rows = []
                for field, col in xm_col_map.items():
                    if isinstance(col, list):
                        status = f"Mapped ({len(col)} cols)"
                        display_col = ", ".join(col)
                    else:
                        status = "Mapped" if col else "Not found"
                        display_col = col or "—"
                    map_rows.append({
                        "Logical Field": field,
                        "Detected Column": display_col,
                        "Status": status,
                    })
                st.dataframe(
                    pd.DataFrame(map_rows),
                    use_container_width=True,
                    hide_index=True,
                )

            # Warnings about missing data points
            xm_warnings = get_missing_data_warnings(xm_col_map)
            for w in xm_warnings:
                st.warning(w, icon="⚠️")

            # Data preview
            with st.expander(f"Source Data Preview ({len(xm_src_df):,} rows)", expanded=False):
                st.dataframe(xm_src_df.head(50), use_container_width=True, height=300)

            # Validation
            if not xm_col_map.get("last_name"):
                st.error("Could not detect a 'Last Name' column. Please check your file format.")
            else:
                available_fields = [f for f, c in xm_col_map.items() if c]
                st.caption(f"Available data points: {', '.join(available_fields)}")

                xm_run = st.button(
                    "Run Cross-DB Match",
                    type="primary",
                    use_container_width=True,
                    key="xmatch_run",
                )

                if xm_run:
                    xm_t0 = time.time()
                    xm_progress = st.progress(0, text="Starting cross-match...")
                    xm_status = st.empty()

                    def _xmatch_progress(pct, msg):
                        xm_progress.progress(min(pct, 99), text=msg)
                        xm_status.caption(msg)

                    xm_result_df = cross_match_batch(
                        xm_src_df, xm_col_map,
                        st.session_state.ref_index,
                        progress_fn=_xmatch_progress,
                    )
                    xm_elapsed = time.time() - xm_t0
                    xm_progress.progress(100, text="Done!")
                    xm_status.empty()

                    st.session_state.xmatch_results = {
                        "result_df": xm_result_df,
                        "elapsed": xm_elapsed,
                        "total_source": len(xm_src_df),
                        "col_map": xm_col_map,
                    }
                    st.rerun()

    # ── Step 3: Results Dashboard ────────────────────────────────────
    if st.session_state.xmatch_results is not None:
        st.markdown("---")
        xr = st.session_state.xmatch_results
        xr_df = xr["result_df"]
        xr_elapsed = xr["elapsed"]
        xr_total = xr["total_source"]

        if xr_df.empty:
            st.info("No valid source records were processed.")
        else:
            xm_dup_ct = int((xr_df["xmatch_verdict"] == "LIKELY_DUP").sum())
            xm_poss_ct = int((xr_df["xmatch_verdict"] == "POSSIBLE_MATCH").sum())
            xm_clean_ct = int((xr_df["xmatch_verdict"] == "CLEAN").sum())

            st.markdown(
                f'<div class="section-header">Cross-Match Results ({xr_elapsed:.1f}s)</div>',
                unsafe_allow_html=True,
            )

            # KPI cards
            xk1, xk2, xk3, xk4, xk5 = st.columns(5)
            with xk1:
                st.markdown(
                    f'<div class="metric-card"><div class="metric-value vid-color">{xr_total:,}</div>'
                    f'<div class="metric-label">Source Records</div></div>',
                    unsafe_allow_html=True,
                )
            with xk2:
                st.markdown(
                    f'<div class="metric-card"><div class="metric-value vid-color">'
                    f'{st.session_state.ref_index.vid_count:,}</div>'
                    f'<div class="metric-label">Master DB Records</div></div>',
                    unsafe_allow_html=True,
                )
            with xk3:
                st.markdown(
                    f'<div class="metric-card"><div class="metric-value" style="color:#ef4444;">'
                    f'{xm_dup_ct:,}</div>'
                    f'<div class="metric-label">Likely Duplicates</div></div>',
                    unsafe_allow_html=True,
                )
            with xk4:
                st.markdown(
                    f'<div class="metric-card"><div class="metric-value review-color">'
                    f'{xm_poss_ct:,}</div>'
                    f'<div class="metric-label">Possible Matches</div></div>',
                    unsafe_allow_html=True,
                )
            with xk5:
                st.markdown(
                    f'<div class="metric-card"><div class="metric-value auto-color">'
                    f'{xm_clean_ct:,}</div>'
                    f'<div class="metric-label">Not in Master DB</div></div>',
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)

            # Effort / overlap banner
            xm_flagged = xm_dup_ct + xm_poss_ct
            if xm_flagged > 0:
                xm_pct = round(xm_flagged / len(xr_df) * 100, 1)
                st.markdown(f"""
                <div class="effort-banner">
                    <div><div class="effort-title">Cross-DB Match Summary</div></div>
                    <div class="effort-divider"></div>
                    <div class="effort-stat">
                        <div class="effort-value" style="color:#ef4444;">{xm_dup_ct:,}</div>
                        <div class="effort-label">Already in Master DB</div>
                    </div>
                    <div class="effort-divider"></div>
                    <div class="effort-stat">
                        <div class="effort-value" style="color:#fbbf24;">{xm_poss_ct:,}</div>
                        <div class="effort-label">Need Review</div>
                    </div>
                    <div class="effort-divider"></div>
                    <div class="effort-stat">
                        <div class="effort-value" style="color:#34d399;">{xm_clean_ct:,}</div>
                        <div class="effort-label">Unique / New</div>
                    </div>
                    <div class="effort-divider"></div>
                    <div class="effort-stat">
                        <div class="effort-value">{xm_pct}%</div>
                        <div class="effort-label">Overlap Rate</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Filtered tabs
            xt_all, xt_dup, xt_poss, xt_clean = st.tabs([
                f"All ({len(xr_df)})",
                f"Likely Duplicates ({xm_dup_ct})",
                f"Possible Matches ({xm_poss_ct})",
                f"Not in Master DB ({xm_clean_ct})",
            ])

            xm_display_cols = [
                "source_vid", "source_link_id", "source_name",
                "source_specialties", "source_city", "source_email",
                "source_affiliation",
                "xmatch_verdict", "match_count",
                "best_match_vid", "best_match_name", "best_match_score",
                "best_match_name_sim", "best_match_rule",
                "best_match_specialties", "best_match_cities",
                "best_match_emails", "best_match_phones",
                "best_match_rationale",
            ]
            xm_display = _filter_cols(xr_df, xm_display_cols)

            with xt_all:
                st.dataframe(xr_df[xm_display], use_container_width=True, height=500)
            with xt_dup:
                df_xdup = xr_df[xr_df["xmatch_verdict"] == "LIKELY_DUP"]
                if not df_xdup.empty:
                    st.dataframe(df_xdup[xm_display], use_container_width=True, height=400)
                else:
                    st.info("No likely duplicates found in master DB.")
            with xt_poss:
                df_xposs = xr_df[xr_df["xmatch_verdict"] == "POSSIBLE_MATCH"]
                if not df_xposs.empty:
                    st.dataframe(df_xposs[xm_display], use_container_width=True, height=400)
                else:
                    st.info("No possible matches found.")
            with xt_clean:
                df_xclean = xr_df[xr_df["xmatch_verdict"] == "CLEAN"]
                if not df_xclean.empty:
                    st.dataframe(df_xclean[xm_display], use_container_width=True, height=400)
                else:
                    st.info("All source records have matches in master DB.")

            # Downloads
            st.markdown("---")
            st.markdown(
                '<div class="section-header">Download Cross-Match Results</div>',
                unsafe_allow_html=True,
            )
            xdl1, xdl2, xdl3 = st.columns(3)
            with xdl1:
                st.download_button(
                    "Download All Results",
                    xr_df.to_csv(index=False),
                    "CrossDB_Match_All.csv",
                    "text/csv",
                    use_container_width=True,
                )
            with xdl2:
                xm_flagged_df = xr_df[xr_df["xmatch_verdict"] != "CLEAN"]
                if not xm_flagged_df.empty:
                    st.download_button(
                        "Download Flagged Only",
                        xm_flagged_df.to_csv(index=False),
                        "CrossDB_Match_Flagged.csv",
                        "text/csv",
                        use_container_width=True,
                    )
            with xdl3:
                xm_buf = io.BytesIO()
                with pd.ExcelWriter(xm_buf, engine="openpyxl") as xmw:
                    write_rules_sheet(xmw, _build_cfg(), entity_filter="hcp")
                    xm_summary = pd.DataFrame([{
                        "total_source_records": len(xr_df),
                        "likely_duplicates": xm_dup_ct,
                        "possible_matches": xm_poss_ct,
                        "clean_records": xm_clean_ct,
                        "master_db_vids": st.session_state.ref_index.vid_count,
                        "screening_time_seconds": round(xr_elapsed, 1),
                    }])
                    xm_summary.to_excel(xmw, index=False, sheet_name="Summary")
                    xr_df.to_excel(xmw, index=False, sheet_name="All_Results")
                    if xm_dup_ct > 0:
                        xr_df[xr_df["xmatch_verdict"] == "LIKELY_DUP"].to_excel(
                            xmw, index=False, sheet_name="Likely_Duplicates",
                        )
                    if xm_poss_ct > 0:
                        xr_df[xr_df["xmatch_verdict"] == "POSSIBLE_MATCH"].to_excel(
                            xmw, index=False, sheet_name="Possible_Matches",
                        )
                    if xm_clean_ct > 0:
                        xr_df[xr_df["xmatch_verdict"] == "CLEAN"].to_excel(
                            xmw, index=False, sheet_name="Clean",
                        )
                st.download_button(
                    "Download Full Excel",
                    xm_buf.getvalue(),
                    "CrossDB_Match.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

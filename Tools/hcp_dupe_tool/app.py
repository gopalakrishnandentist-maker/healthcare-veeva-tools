"""
app.py — Streamlit GUI for the HCP/HCO Duplicate Identification Tool.

Launch with:
    streamlit run app.py

Features:
    - Drag-and-drop file upload (CSV or Excel)
    - Configurable thresholds via sidebar
    - Live progress indicators
    - Interactive results dashboard with charts
    - One-click download of all outputs
"""

from __future__ import annotations

import io
import os
import sys
import time
import tempfile
import logging
from collections import Counter

import pandas as pd
import streamlit as st

# ── Ensure package is importable ─────────────────────────────────────
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(PACKAGE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from hcp_dupe_tool.core import (
    BlockingEngine,
    DSU,
    SharedContactDetector,
    name_similarity,
    norm_text,
)
from hcp_dupe_tool.hcp_pipeline import run_hcp_pipeline
from hcp_dupe_tool.run import _load_config, _hardcoded_defaults

# ── GK.Ai shared theme ───────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
from gkai_theme import inject_gkai_theme, GKAI_PAGE_CONFIG, render_app_header, render_sidebar_nav

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    **GKAI_PAGE_CONFIG,
    page_title="HCP Dupe Check",
    page_icon="🔍",
)
inject_gkai_theme()

# ── Header ───────────────────────────────────────────────────────────
render_app_header(
    title="HCP Duplicate Identification Tool",
    description="Upload your Veeva extract to detect duplicate Healthcare Professional records",
    tags=[{"label": "v2.0", "color": "green"}],
)


# ── Sidebar: Configuration ───────────────────────────────────────────
with st.sidebar:
    render_sidebar_nav(app_title="HCP Dupe Check", subtitle="GK.Ai", version="v2.0")

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
    st.markdown("#### About")
    st.caption("v2.0 | Built for Veeva OpenData extracts")
    st.caption("Supports: CSV, XLSX")


# ── File Input (Upload OR File Path) ─────────────────────────────────
input_method = st.radio(
    "How do you want to load your data?",
    ["Upload file (< 200MB)", "Enter file path (for large files)"],
    horizontal=True,
)

uploaded_file = None
file_path_input = None

if input_method == "Upload file (< 200MB)":
    col_upload, col_info = st.columns([2, 1])
    with col_upload:
        uploaded_file = st.file_uploader(
            "Upload your HCP extract",
            type=["csv", "xlsx", "xls"],
            help="Drag and drop your Veeva export file here"
        )
    with col_info:
        if uploaded_file is None:
            st.markdown("""
            **Supported formats:** CSV, Excel
            **Required columns:**
            - `hcp.vid__v (VID)`
            - `hcp.last_name__v (LAST NAME)`

            **Recommended columns:**
            - First name, Specialty, License
            - Phone, Email, Address
            - HCO affiliation
            """)
else:
    file_path_input = st.text_input(
        "Full path to your CSV or Excel file",
        placeholder="/Users/you/Desktop/Prompts/hcp_dupe_tool/Data/your_file.csv",
        help="Paste the full file path here. CSV recommended for large files (faster loading)."
    )
    if file_path_input and not os.path.isfile(file_path_input):
        st.warning(f"File not found: `{file_path_input}`")
        file_path_input = None

# ── Session state ────────────────────────────────────────────────────
if "results" not in st.session_state:
    st.session_state.results = None
if "run_time" not in st.session_state:
    st.session_state.run_time = None


# ── Run Pipeline ─────────────────────────────────────────────────────
has_data = uploaded_file is not None or (file_path_input is not None and file_path_input)

if has_data:
    # Load data
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

    with st.spinner("Loading data..."):
        if uploaded_file is not None:
            df = load_data_upload(uploaded_file.getvalue(), uploaded_file.name)
        else:
            df = load_data_path(file_path_input)

    # Data preview
    with st.expander(f"Data Preview — {len(df):,} rows x {len(df.columns)} columns", expanded=False):
        st.dataframe(df.head(50), use_container_width=True, height=300)

    # Column detection summary
    vid_col = "hcp.vid__v (VID)"
    if vid_col not in df.columns:
        st.error(f"Required column `{vid_col}` not found. Available columns: {', '.join(df.columns[:10])}...")
        st.stop()

    unique_vids = df[vid_col].nunique()
    phone_cols = len([c for c in df.columns if c.startswith("hcp.phone_")])
    email_cols = len([c for c in df.columns if c.startswith("hcp.email_")])
    spec_cols = len([c for c in df.columns if c.startswith("hcp.specialty_")])

    # Auto-detect large dataset
    is_large = len(df) > 50_000
    if is_large:
        st.info(f"Large dataset detected ({len(df):,} rows). Blocking will use tighter defaults for performance.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Raw Rows", f"{len(df):,}")
    col2.metric("Unique VIDs", f"{unique_vids:,}")
    col3.metric("Phone/Email Cols", f"{phone_cols} / {email_cols}")
    col4.metric("Specialty Cols", f"{spec_cols}")

    # Run button
    st.markdown("---")
    run_clicked = st.button("Run Duplicate Detection", type="primary", use_container_width=True)

    if run_clicked:
        # Build config — auto-tune for large datasets
        cfg = _hardcoded_defaults()
        cfg["shared_contact"]["threshold"] = shared_threshold
        cfg["blocking"]["max_block_size"] = max_block if not is_large else min(max_block, 300)
        cfg["blocking"]["phonetic_blocking"] = phonetic if not is_large else False
        cfg["blocking"]["first_initial_blocking"] = first_initial if not is_large else False
        cfg["name_matching"]["strong"] = name_strong
        cfg["name_matching"]["medium"] = name_medium
        cfg["hcp_review_scoring"]["review_threshold"] = review_threshold
        cfg["output"]["enrich_output"] = True

        # Run with step-by-step progress
        t0 = time.time()
        progress = st.progress(0, text="Initializing...")
        status_text = st.empty()

        def update_progress(pct, msg):
            progress.progress(pct, text=msg)
            status_text.caption(f"{msg} ({time.time() - t0:.0f}s elapsed)")

        update_progress(5, "Canonicalizing records...")
        results = run_hcp_pipeline(df, cfg)
        update_progress(95, "Finalizing results...")
        time.sleep(0.2)
        update_progress(100, "Done!")

        elapsed = time.time() - t0
        st.session_state.results = results
        st.session_state.run_time = elapsed
        st.rerun()

# ── Results Dashboard ────────────────────────────────────────────────
if st.session_state.results is not None:
    results = st.session_state.results
    elapsed = st.session_state.run_time or 0

    hcp_auto = results.get("hcp_auto", pd.DataFrame())
    hcp_review = results.get("hcp_review", pd.DataFrame())
    hcp_notdup = results.get("hcp_notdup", pd.DataFrame())
    hcp_clusters = results.get("hcp_clusters", pd.DataFrame())
    hcp_shared = results.get("hcp_shared", pd.DataFrame())
    hcp_summary = results.get("hcp_summary", pd.DataFrame())

    auto_ct = len(hcp_auto)
    review_ct = len(hcp_review)
    notdup_ct = len(hcp_notdup)
    cluster_ct = hcp_clusters["cluster_id"].nunique() if not hcp_clusters.empty and "cluster_id" in hcp_clusters.columns else 0
    total_pairs = auto_ct + review_ct + notdup_ct

    st.success(f"Completed in {elapsed:.1f}s")

    # ── KPI Cards ────────────────────────────────────────────────────
    st.markdown("### Results Overview")

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value vid-color">{hcp_summary.iloc[0].get('unique_hcp_vids', 0) if not hcp_summary.empty else 0:,.0f}</div>
            <div class="metric-label">Unique VIDs</div>
        </div>""", unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value pair-color">{total_pairs:,}</div>
            <div class="metric-label">Pairs Evaluated</div>
        </div>""", unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value auto-color">{auto_ct:,}</div>
            <div class="metric-label">Auto-Merge</div>
        </div>""", unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value review-color">{review_ct:,}</div>
            <div class="metric-label">Manual Review</div>
        </div>""", unsafe_allow_html=True)

    with c5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value notdup-color">{notdup_ct:,}</div>
            <div class="metric-label">Not Duplicate</div>
        </div>""", unsafe_allow_html=True)

    with c6:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value cluster-color">{cluster_ct:,}</div>
            <div class="metric-label">Clusters</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts Row ───────────────────────────────────────────────────
    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
        st.markdown("##### Classification Breakdown")
        chart_data = pd.DataFrame({
            "Category": ["Auto-Merge", "Review", "Not Duplicate"],
            "Count": [auto_ct, review_ct, notdup_ct],
        })
        if total_pairs > 0:
            import altair as alt
            pie = alt.Chart(chart_data).mark_arc(innerRadius=50, outerRadius=100).encode(
                theta=alt.Theta("Count:Q"),
                color=alt.Color("Category:N", scale=alt.Scale(
                    domain=["Auto-Merge", "Review", "Not Duplicate"],
                    range=["#16a34a", "#d97706", "#94a3b8"]
                )),
                tooltip=["Category", "Count"],
            ).properties(height=250)
            st.altair_chart(pie, use_container_width=True)
        else:
            st.info("No pairs to visualize")

    with chart_col2:
        st.markdown("##### Auto-Merge Rules")
        if not hcp_auto.empty and "rule" in hcp_auto.columns:
            rule_counts = hcp_auto["rule"].value_counts().reset_index()
            rule_counts.columns = ["Rule", "Count"]

            rule_descriptions = {
                "G1_NAME_SPL_HCO": "Name+Spec+HCO",
                "G2_NAME_SPL_PIN": "Name+Spec+PIN",
                "G3_NAME_SPL_CITY": "Name+Spec+City",
                "G4_LICENSE": "License Match",
                "G5_PHONE_EMAIL": "Phone+Email",
                "G6_EMAIL_NAME": "Email+Name",
            }
            rule_counts["Label"] = rule_counts["Rule"].map(
                lambda x: rule_descriptions.get(x, x)
            )

            bar = alt.Chart(rule_counts).mark_bar(
                cornerRadiusTopRight=6,
                cornerRadiusBottomRight=6,
            ).encode(
                x=alt.X("Count:Q", title="Pairs"),
                y=alt.Y("Label:N", sort="-x", title=""),
                color=alt.value("#2563eb"),
                tooltip=["Label", "Count"],
            ).properties(height=250)
            st.altair_chart(bar, use_container_width=True)
        else:
            st.info("No auto-merge pairs")

    with chart_col3:
        st.markdown("##### Name Similarity Distribution")
        all_sims = []
        for frame, label in [(hcp_auto, "Auto"), (hcp_review, "Review"), (hcp_notdup, "Not-Dup")]:
            if not frame.empty and "name_similarity" in frame.columns:
                sims = frame["name_similarity"].dropna().astype(float)
                for s in sims:
                    all_sims.append({"Similarity": s, "Category": label})

        if all_sims:
            sim_df = pd.DataFrame(all_sims)
            hist = alt.Chart(sim_df).mark_bar(opacity=0.7).encode(
                x=alt.X("Similarity:Q", bin=alt.Bin(maxbins=20), title="Name Similarity %"),
                y=alt.Y("count()", title="Pairs"),
                color=alt.Color("Category:N", scale=alt.Scale(
                    domain=["Auto", "Review", "Not-Dup"],
                    range=["#16a34a", "#d97706", "#94a3b8"]
                )),
                tooltip=["Category", "count()"],
            ).properties(height=250)
            st.altair_chart(hist, use_container_width=True)
        else:
            st.info("No data for histogram")

    # ── Detailed Tabs ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Detailed Results")

    tab_auto, tab_review, tab_notdup, tab_clusters, tab_shared = st.tabs([
        f"Auto-Merge ({auto_ct})",
        f"Review ({review_ct})",
        f"Not Duplicate ({notdup_ct})",
        f"Clusters ({cluster_ct})",
        f"Shared Contacts ({len(hcp_shared)})",
    ])

    display_cols_auto = [
        "vid_a", "vid_b", "rule", "name_similarity",
        "name_a", "name_b", "specialty_match",
        "cities_a", "cities_b", "comments",
    ]
    display_cols_review = [
        "vid_a", "vid_b", "score", "name_similarity",
        "name_a", "name_b", "specialty_match",
        "cities_a", "cities_b", "reasons", "comments",
    ]
    display_cols_notdup = [
        "vid_a", "vid_b", "reason", "name_similarity",
        "name_a", "name_b", "specialty_match",
        "cities_a", "cities_b", "comments",
    ]

    def _filter_cols(df, cols):
        return [c for c in cols if c in df.columns]

    with tab_auto:
        if not hcp_auto.empty:
            # Rule filter
            rules = ["All"] + sorted(hcp_auto["rule"].unique().tolist()) if "rule" in hcp_auto.columns else ["All"]
            selected_rule = st.selectbox("Filter by rule", rules, key="auto_rule")
            filtered = hcp_auto if selected_rule == "All" else hcp_auto[hcp_auto["rule"] == selected_rule]

            st.dataframe(
                filtered[_filter_cols(filtered, display_cols_auto)],
                use_container_width=True,
                height=400,
            )
        else:
            st.info("No auto-merge pairs found.")

    with tab_review:
        if not hcp_review.empty:
            # Score filter
            min_score = int(hcp_review["score"].min()) if "score" in hcp_review.columns else 0
            max_score = int(hcp_review["score"].max()) if "score" in hcp_review.columns else 100
            score_range = st.slider(
                "Score range", min_score, max_score, (min_score, max_score), key="review_score"
            )
            filtered = hcp_review
            if "score" in hcp_review.columns:
                filtered = hcp_review[
                    (hcp_review["score"].astype(float) >= score_range[0]) &
                    (hcp_review["score"].astype(float) <= score_range[1])
                ]
            st.dataframe(
                filtered[_filter_cols(filtered, display_cols_review)],
                use_container_width=True,
                height=400,
            )
        else:
            st.info("No review pairs found.")

    with tab_notdup:
        if not hcp_notdup.empty:
            st.dataframe(
                hcp_notdup[_filter_cols(hcp_notdup, display_cols_notdup)],
                use_container_width=True,
                height=400,
            )
        else:
            st.info("No not-duplicate pairs.")

    with tab_clusters:
        if not hcp_clusters.empty:
            cluster_ids = sorted(hcp_clusters["cluster_id"].unique().tolist()) if "cluster_id" in hcp_clusters.columns else []
            if cluster_ids:
                selected_cluster = st.selectbox(
                    "Select cluster", ["All"] + [str(c) for c in cluster_ids], key="cluster_select"
                )
                filtered = hcp_clusters if selected_cluster == "All" else hcp_clusters[
                    hcp_clusters["cluster_id"].astype(str) == selected_cluster
                ]
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

    # ── Download Section ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Download Results")

    dl_col1, dl_col2, dl_col3, dl_col4 = st.columns(4)

    with dl_col1:
        if not hcp_auto.empty:
            csv = hcp_auto.to_csv(index=False)
            st.download_button(
                "Download Auto-Merge",
                csv,
                "HCP_AUTO_MERGE.csv",
                "text/csv",
                use_container_width=True,
            )

    with dl_col2:
        if not hcp_review.empty:
            csv = hcp_review.to_csv(index=False)
            st.download_button(
                "Download Review",
                csv,
                "HCP_REVIEW.csv",
                "text/csv",
                use_container_width=True,
            )

    with dl_col3:
        if not hcp_notdup.empty:
            csv = hcp_notdup.to_csv(index=False)
            st.download_button(
                "Download Not-Dup",
                csv,
                "HCP_NOT_DUP.csv",
                "text/csv",
                use_container_width=True,
            )

    with dl_col4:
        # All-in-one Excel
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as w:
            if not hcp_summary.empty:
                hcp_summary.to_excel(w, index=False, sheet_name="Summary")
            if not hcp_auto.empty:
                hcp_auto.to_excel(w, index=False, sheet_name="AUTO_MERGE")
            if not hcp_review.empty:
                hcp_review.to_excel(w, index=False, sheet_name="REVIEW")
            if not hcp_notdup.empty:
                hcp_notdup.to_excel(w, index=False, sheet_name="NOT_DUP")
            if not hcp_clusters.empty:
                hcp_clusters.to_excel(w, index=False, sheet_name="CLUSTERS")
            if not hcp_shared.empty:
                hcp_shared.to_excel(w, index=False, sheet_name="Shared_Contacts")

        st.download_button(
            "Download Full Excel",
            buffer.getvalue(),
            "HCP_Dupe_Check.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

elif uploaded_file is None:
    # Landing state — show feature cards
    st.markdown("<br>", unsafe_allow_html=True)

    f1, f2, f3 = st.columns(3)
    with f1:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value" style="font-size:2rem;">📊</div>
            <div class="metric-label" style="font-size:0.95rem; color:#1e293b; margin-top:0.5rem;">Smart Matching</div>
            <p style="font-size:0.8rem; color:#64748b; margin-top:0.3rem;">
                Name + Specialty + Affiliation<br>
                Fuzzy matching catches spelling variants<br>
                License conflict detection
            </p>
        </div>
        """, unsafe_allow_html=True)

    with f2:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value" style="font-size:2rem;">⚡</div>
            <div class="metric-label" style="font-size:0.95rem; color:#1e293b; margin-top:0.5rem;">Tiered Classification</div>
            <p style="font-size:0.8rem; color:#64748b; margin-top:0.3rem;">
                Auto-Merge: High confidence pairs<br>
                Review: Needs human judgment<br>
                Not-Dup: Safely excluded
            </p>
        </div>
        """, unsafe_allow_html=True)

    with f3:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value" style="font-size:2rem;">🔒</div>
            <div class="metric-label" style="font-size:0.95rem; color:#1e293b; margin-top:0.5rem;">Shared Contact Aware</div>
            <p style="font-size:0.8rem; color:#64748b; margin-top:0.3rem;">
                Detects hospital switchboard numbers<br>
                Prevents false positives<br>
                Configurable thresholds
            </p>
        </div>
        """, unsafe_allow_html=True)

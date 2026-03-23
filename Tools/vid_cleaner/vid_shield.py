"""
VID Data Shield v2.1 — Streamlit App
Prevents Excel scientific notation corruption of 18-digit Veeva IDs.
Built for Veeva OpenData India Operations.

Supports files of any size — no upload limit.
For 800 MB+ files, use the local file path option for best performance.
"""

import streamlit as st
import pandas as pd
import io
import re
import os
import time

# ── GK.Ai shared theme ──────────────────────────────────────
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from gkai_theme import inject_gkai_theme, GKAI_PAGE_CONFIG, render_app_header, render_sidebar_nav

st.set_page_config(
    **GKAI_PAGE_CONFIG,
    page_title="VID Data Shield",
    page_icon="https://img.icons8.com/fluency/48/shield.png",
)
inject_gkai_theme()

# ── Hero ─────────────────────────────────────────────────────
render_app_header(
    title="VID Data Shield",
    description="Protect 18-digit Veeva IDs from Excel scientific notation corruption",
    tags=[{"label": "v2.1", "color": "green"}],
)


# ── How it works ─────────────────────────────────────────────
with st.expander("How it works", expanded=False):
    st.markdown("""
<div class="before-after">
    <div class="ba-card ba-before">
        <strong>BEFORE (Broken)</strong><br><br>
        VID: 938488000012345678<br>
        Excel shows: <strong>9.38488E+17</strong><br><br>
        Data silently corrupted. Lookups fail.
    </div>
    <div class="ba-card ba-after">
        <strong>AFTER (Fixed)</strong><br><br>
        VID: 938488000012345678<br>
        Excel shows: <strong>938488000012345678</strong><br><br>
        IDs preserved as text. All lookups work.
    </div>
</div>
    """, unsafe_allow_html=True)


# ── Input Mode ───────────────────────────────────────────────
st.markdown("")
input_mode = st.radio(
    "Choose input method:",
    ["Upload file (drag & drop)", "Local file path (best for large files 800 MB+)"],
    horizontal=True,
    help="For files over 800 MB, use the local path option — it reads directly from disk without uploading through the browser."
)

df = None
file_label = None

if input_mode == "Upload file (drag & drop)":
    uploaded_file = st.file_uploader(
        "Upload your CSV or Excel file",
        type=["csv", "xlsx", "xls"],
        help="Drag and drop your Veeva export here — no file size limit"
    )
    if uploaded_file is not None:
        with st.spinner("Loading file..."):
            t0 = time.time()
            if uploaded_file.name.lower().endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, low_memory=False)
            else:
                df = pd.read_excel(uploaded_file, dtype=str)
            elapsed = time.time() - t0
        file_label = uploaded_file.name
        st.success(f"Loaded **{len(df):,}** rows x **{len(df.columns)}** columns  ({elapsed:.1f}s)")

else:
    local_path = st.text_input(
        "Paste the full file path:",
        placeholder="/Users/you/Desktop/Prompts/Healthcare/Tools/export.csv",
        help="Supports .csv, .xlsx, .xls — reads directly from disk, no size limit"
    )
    if local_path and local_path.strip():
        local_path = local_path.strip().strip('"').strip("'")
        if not os.path.isfile(local_path):
            st.error(f"File not found: `{local_path}`")
        elif not local_path.lower().endswith((".csv", ".xlsx", ".xls")):
            st.error("Unsupported format. Please use .csv, .xlsx, or .xls")
        else:
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            st.info(f"File size: **{file_size_mb:,.1f} MB** — reading from disk...")
            with st.spinner(f"Loading {file_size_mb:,.0f} MB file..."):
                t0 = time.time()
                if local_path.lower().endswith(".csv"):
                    df = pd.read_csv(local_path, dtype=str, low_memory=False)
                else:
                    df = pd.read_excel(local_path, dtype=str)
                elapsed = time.time() - t0
            file_label = os.path.basename(local_path)
            st.success(f"Loaded **{len(df):,}** rows x **{len(df.columns)}** columns  ({elapsed:.1f}s)")

if df is not None:

    # Column picker
    columns = list(df.columns)
    default_idx = 0
    for i, col in enumerate(columns):
        if "vid" in str(col).lower():
            default_idx = i
            break

    col1, col2 = st.columns([2, 1])
    with col1:
        id_column = st.selectbox(
            "Select the column containing VIDs:",
            columns,
            index=default_idx
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        process = st.button("Process & Protect", type="primary", use_container_width=True)

    # Preview
    with st.expander("Data Preview", expanded=False):
        st.dataframe(df.head(20), use_container_width=True, height=300)

    # Process
    if process:
        total_ids = len(df)
        progress_bar = st.progress(0, text="Analyzing VIDs...")

        # Analyze
        original_values = df[id_column].astype(str)
        long_ids = original_values.apply(lambda x: len(re.sub(r'[^0-9]', '', str(x))) >= 15)
        scientific = original_values.str.contains(r'[eE]\+', na=False)
        at_risk = (long_ids | scientific).sum()

        progress_bar.progress(20, text="Formatting IDs as text...")

        # Force to text string
        df[id_column] = df[id_column].astype(str).str.strip()

        progress_bar.progress(30, text="Building protected Excel file...")

        # Build Excel output with text formatting
        t0 = time.time()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Protected")
            workbook = writer.book
            worksheet = writer.sheets["Protected"]
            text_fmt = workbook.add_format({"num_format": "@"})
            col_idx = df.columns.get_loc(id_column)
            worksheet.set_column(col_idx, col_idx, 25, text_fmt)

            # Write each cell as explicit text string — with progress for large files
            chunk = max(1, total_ids // 10)
            for row_num, val in enumerate(df[id_column], start=1):
                worksheet.write_string(row_num, col_idx, str(val), text_fmt)
                if row_num % chunk == 0:
                    pct = 30 + int((row_num / total_ids) * 60)
                    progress_bar.progress(min(pct, 90), text=f"Writing row {row_num:,} of {total_ids:,}...")

        processed_data = output.getvalue()
        elapsed = time.time() - t0
        progress_bar.progress(100, text=f"Done in {elapsed:.1f}s")

        # Results
        st.markdown("""
        <div class="success-banner">
            <h3>VIDs Protected Successfully</h3>
            <p>All IDs in the selected column are now formatted as text strings in the output Excel file.</p>
        </div>
        """, unsafe_allow_html=True)

        # Stats
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value blue">{total_ids:,}</div>
                <div class="stat-label">Total Records</div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value orange">{at_risk:,}</div>
                <div class="stat-label">IDs at Risk (15+ digits)</div>
            </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value green">{total_ids:,}</div>
                <div class="stat-label">IDs Protected</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("")

        # Download options
        base_name = file_label.rsplit('.', 1)[0] if file_label else "output"
        out_filename = f"Protected_{base_name}.xlsx"

        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            st.download_button(
                label="Download Protected Excel",
                data=processed_data,
                file_name=out_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )
        with dl_col2:
            save_path = st.text_input(
                "Or save directly to a local path:",
                placeholder="/Users/you/Desktop/Protected_output.xlsx",
                help="For large files, saving locally is faster than downloading through the browser"
            )
            if save_path and save_path.strip():
                save_path = save_path.strip().strip('"').strip("'")
                if st.button("Save to Disk", use_container_width=True):
                    try:
                        with open(save_path, "wb") as f:
                            f.write(processed_data)
                        st.success(f"Saved to `{save_path}` ({len(processed_data)/(1024*1024):,.1f} MB)")
                    except Exception as e:
                        st.error(f"Could not save: {e}")

else:
    # Empty state
    st.markdown("")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value blue">1</div>
            <div class="stat-label">Upload your file</div>
            <p style="font-size:0.8rem;color:#94a3b8;margin-top:0.5rem;">CSV or Excel with VID column</p>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value blue">2</div>
            <div class="stat-label">Select VID column</div>
            <p style="font-size:0.8rem;color:#94a3b8;margin-top:0.5rem;">Auto-detects columns with "vid"</p>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value blue">3</div>
            <div class="stat-label">Download protected file</div>
            <p style="font-size:0.8rem;color:#94a3b8;margin-top:0.5rem;">Excel with text-formatted IDs</p>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.caption("VID Data Shield v2.1 | Built for Veeva OpenData India Operations | All processing runs locally | No file size limit")

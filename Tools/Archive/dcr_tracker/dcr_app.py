#!/usr/bin/env python3
"""
DCR Master Tracker Updater — Streamlit App
-------------------------------------------
Upload your Veeva DCR report + Master Tracker → Get deduplicated, updated tracker.
Also generates daily JIRA update content from tracker data.

Run:  streamlit run dcr_app.py --server.port 8510
"""

import csv
import io
import re
import zipfile
from datetime import datetime, date

import pandas as pd
import streamlit as st

# ── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DCR Tracker Updater",
    page_icon="📊",
    layout="wide",
)

# ── Resolution Code → Reason Mapping ──────────────────────────────────────
CODE_TO_REASON = {
    "[A-00013]": "N/A",
    "[A-10017]": "N/A",
    "[A-10018]": "N/A",
    "[A-10019]": "N/A",
    "[A-10022]": "N/A",
    "[A-10024]": "N/A",
    "[A-10025]": "N/A",
    "[R-00015]": "Auto Rejections by System",
    "[R-00102]": "N/A",
    "[R-10001]": "Missing/Unclear info.",
    "[R-10004]": "Failed validation",
    "[R-10050]": "Missing/Unclear info.",
    "[R-10053]": "Inheritance rule",
    "[R-10054]": "Request on wrong record",
    "[R-10055]": "Missing/Unclear info.",
    "[R-10056]": "Incorrect info/request",
    "[R-10057]": "Opted-out",
    "[R-10058]": "Out of OD scope",
    "[R-10060]": "Confusion on entity type",
    "[R-10061]": "Confusion on entity type",
    "[R-10064]": "Request on personal info.",
    "[R-10070]": "Future changes",
    "[R-10072]": "Failed validation",
}

# Source name replacements for JIRA
SOURCE_REPLACEMENTS = {
    "OpenData API": "Sanofi",
    "Veeva CRM": "Takeda",
}

# ── Tracker Column Order ──────────────────────────────────────────────────
TRACKER_COLUMNS = [
    "CREATED DATE",
    "COMPLETED DATE",
    "Service Time",
    "Entity Link",
    "Task Link",
    "hcp.vid__v (VID)",
    "change_request.change_request_id (CHANGE REQUEST ID)",
    "change_request.subject (SUBJECT)",
    "hcp.type (TYPE)",
    "hcp.specialty (SPECIALTY)",
    "change_request.source (SOURCE)",
    "change_request.originating_system (ORIGINATING SYSTEM)",
    "change_request.created_by (CREATED BY)",
    "change_request.change_request_type (TYPE)",
    "change_request.owner (OWNER)",
    "CREATED DATE",      # col 16 duplicate
    "COMPLETED DATE",    # col 17 duplicate
    "change_request.resolution (RESOLUTION)",
    "RESOLUTION",
    "Resolution Note Code",
    "Reason for Rejection",
    "Resolution Note_Trimmed",
    "change_request.resolution_notes (RESOLUTION NOTES)",
    "Month",
]


# ── Helper Functions ──────────────────────────────────────────────────────
def compute_service_time(created_str, completed_str):
    """Calculate service time as H:MM:SS."""
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        created = datetime.strptime(str(created_str).strip(), fmt)
        completed = datetime.strptime(str(completed_str).strip(), fmt)
        delta = completed - created
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0:
            return ""
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    except (ValueError, AttributeError):
        return ""


def compute_month(created_str):
    """Extract YYYY-MM from created date."""
    try:
        return str(created_str).strip()[:7]
    except (AttributeError, IndexError):
        return ""


def extract_resolution_note_code(resolution_notes):
    """Extract the last [X-NNNNN] code from resolution notes."""
    codes = re.findall(r"\[[A-Z]-\d{5}\]", str(resolution_notes or ""))
    return codes[-1] if codes else ""


def extract_resolution_note_trimmed(resolution_notes, code):
    """Extract text from the last resolution code onwards."""
    if not code or not resolution_notes:
        return ""
    notes_str = str(resolution_notes)
    idx = notes_str.rfind(code)
    if idx >= 0:
        return notes_str[idx:]
    return ""


def lookup_rejection_reason(code):
    """Map a resolution note code to its rejection reason."""
    return CODE_TO_REASON.get(code, "N/A")


def replace_source_name(source):
    """Apply global source name replacements for JIRA."""
    return SOURCE_REPLACEMENTS.get(source, source)


def is_weekday(date_str):
    """Check if a date string (YYYY-MM-DD ...) falls on a weekday (Mon-Fri)."""
    try:
        d = datetime.strptime(str(date_str).strip()[:10], "%Y-%m-%d")
        return d.weekday() < 5  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    except (ValueError, AttributeError):
        return False


def parse_service_time_hours(service_time_val):
    """Convert service time (string H:MM:SS or Timedelta) to hours as float."""
    if service_time_val is None:
        return None
    if pd.isna(service_time_val):
        return None
    # Handle pandas Timedelta
    if isinstance(service_time_val, pd.Timedelta):
        return service_time_val.total_seconds() / 3600
    # Handle datetime.timedelta
    from datetime import timedelta
    if isinstance(service_time_val, timedelta):
        return service_time_val.total_seconds() / 3600
    # Handle string H:MM:SS
    s = str(service_time_val)
    if ":" not in s:
        return None
    parts = s.split(":")
    try:
        return int(parts[0]) + int(parts[1]) / 60 + int(parts[2]) / 3600
    except (ValueError, IndexError):
        return None


def format_hours_hhmm(hours_float):
    """Format hours as Xh Ym string."""
    h = int(hours_float)
    m = int((hours_float - h) * 60)
    return f"{h}h {m}m"


def dedupe_columns(columns):
    """Make duplicate column names unique by appending _2, _3, etc."""
    seen = {}
    result = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            result.append(col)
    return result


def read_uploaded_csv(uploaded_file):
    """Read an uploaded CSV file into a list of rows (header + data)."""
    content = uploaded_file.read().decode("utf-8")
    uploaded_file.seek(0)
    reader = csv.reader(io.StringIO(content))
    return list(reader)


def read_uploaded_zip(uploaded_file):
    """Extract and read the CSV from an uploaded zip file."""
    with zipfile.ZipFile(io.BytesIO(uploaded_file.read())) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            return None
        with zf.open(csv_names[0]) as cf:
            content = cf.read().decode("utf-8")
            reader = csv.reader(io.StringIO(content))
            return list(reader)


def transform_daily_row(header_map, row):
    """Transform a daily report row into tracker format."""
    def get(col):
        idx = header_map.get(col)
        if idx is not None and idx < len(row):
            return row[idx]
        return ""

    vid = get("change_request_hcp.vid__v (VID)")
    cr_id = get("change_request.change_request_id (CHANGE REQUEST ID)")
    subject = get("change_request.subject (SUBJECT)")
    hcp_type = get("TYPE")
    specialty = get("SPECIALTY")
    source = get("change_request.source (SOURCE)")
    orig_sys = get("change_request.originating_system (ORIGINATING SYSTEM)")
    created_by = get("change_request.created_by (CREATED BY)")
    cr_type = get("change_request.change_request_type (TYPE)")
    owner = get("change_request.owner (OWNER)")
    created_date = get("CREATED DATE")
    completed_date = get("COMPLETED DATE")
    resolution = get("RESOLUTION")
    resolution_notes = get("change_request.resolution_notes (RESOLUTION NOTES)")

    # Derived columns
    service_time = compute_service_time(created_date, completed_date)
    month = compute_month(created_date)
    note_code = extract_resolution_note_code(resolution_notes)
    note_trimmed = extract_resolution_note_trimmed(resolution_notes, note_code)
    rejection_reason = lookup_rejection_reason(note_code)

    return [
        created_date,       # Col 1: CREATED DATE
        completed_date,     # Col 2: COMPLETED DATE
        service_time,       # Col 3: Service Time
        vid,                # Col 4: Entity Link
        cr_id,              # Col 5: Task Link
        vid,                # Col 6: hcp.vid__v (VID)
        cr_id,              # Col 7: change_request.change_request_id
        subject,            # Col 8: Subject
        hcp_type,           # Col 9: TYPE
        specialty,          # Col 10: SPECIALTY
        source,             # Col 11: Source
        orig_sys,           # Col 12: Originating System
        created_by,         # Col 13: Created By
        cr_type,            # Col 14: Change Request Type
        owner,              # Col 15: Owner
        created_date,       # Col 16: CREATED DATE (dup)
        completed_date,     # Col 17: COMPLETED DATE (dup)
        resolution,         # Col 18: Resolution
        resolution,         # Col 19: RESOLUTION
        note_code,          # Col 20: Resolution Note Code
        rejection_reason,   # Col 21: Reason for Rejection
        note_trimmed,       # Col 22: Resolution Note_Trimmed
        resolution_notes,   # Col 23: Resolution Notes
        month,              # Col 24: Month
    ]


def rows_to_csv_bytes(header, rows):
    """Convert header + rows to CSV bytes for download."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


# ── JIRA Content Generator ───────────────────────────────────────────────
def generate_jira_content(df, start_date, end_date, pending_count):
    """Generate JIRA-formatted update from tracker dataframe for a date range."""
    if start_date == end_date:
        display_date = start_date.strftime("%d-%b-%Y")
    else:
        display_date = f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}"

    # Filter rows completed within the date range (inclusive)
    def date_in_range(completed_str):
        try:
            d = completed_str[:10]
            return start_date.strftime("%Y-%m-%d") <= d <= end_date.strftime("%Y-%m-%d")
        except (TypeError, IndexError):
            return False

    day_df = df[df["COMPLETED DATE"].apply(date_in_range)].copy()

    if day_df.empty:
        return None, f"No data found for {display_date}"

    # Apply source replacements
    day_df["Source_Display"] = day_df["change_request.source (SOURCE)"].apply(replace_source_name)

    # ── Overall Summary ──────────────────────────────────────────────
    total = len(day_df)
    accepted = len(day_df[day_df["RESOLUTION"] == "Accepted"])
    rejected = len(day_df[day_df["RESOLUTION"] == "Rejected"])
    acc_pct = (accepted / total * 100) if total > 0 else 0
    rej_pct = (rejected / total * 100) if total > 0 else 0

    # SLA calculations (weekdays only — weekend DCRs excluded from SLA)
    weekday_mask = day_df["COMPLETED DATE"].apply(is_weekday)
    weekday_df = day_df[weekday_mask]
    sla_hours = weekday_df["Service Time"].apply(parse_service_time_hours).dropna()
    missed_sla = int((sla_hours > 24).sum()) if len(sla_hours) > 0 else 0
    avg_sla = sla_hours.mean() if len(sla_hours) > 0 else 0

    # ── Client-wise Breakdown ────────────────────────────────────────
    client_data = []
    for source in sorted(day_df["Source_Display"].unique()):
        src_df = day_df[day_df["Source_Display"] == source]
        src_total = len(src_df)
        src_acc = len(src_df[src_df["RESOLUTION"] == "Accepted"])
        src_rej = len(src_df[src_df["RESOLUTION"] == "Rejected"])
        src_acc_rate = f"{src_acc / src_total * 100:.1f}%" if src_total > 0 else "0%"
        src_rej_rate = f"{src_rej / src_total * 100:.1f}%" if src_total > 0 else "0%"
        client_data.append({
            "Source": source,
            "Accepted": src_acc,
            "Rejected": src_rej,
            "Grand Total": src_total,
            "Approval Rate": src_acc_rate,
            "Rejection Rate": src_rej_rate,
        })

    # ── Rejection Reasons ────────────────────────────────────────────
    rej_df = day_df[day_df["RESOLUTION"] == "Rejected"]
    rejection_reasons = {}
    if len(rej_df) > 0:
        reason_counts = rej_df["Reason for Rejection"].value_counts()
        for reason, count in reason_counts.items():
            if reason and reason != "N/A" and reason != "#N/A":
                rejection_reasons[reason] = int(count)

    # ── Build JIRA Text ──────────────────────────────────────────────
    lines = []
    lines.append(f"*Overall Summary*")
    lines.append(f"Total DCRs Processed: {total}")
    lines.append(f"Approved: {accepted} ({acc_pct:.1f}%)")
    lines.append(f"Rejected: {rejected} ({rej_pct:.1f}%)")
    lines.append(f"Missed SLA: {missed_sla}")
    lines.append(f"Average SLA: {format_hours_hhmm(avg_sla)}")
    lines.append("")

    # Client table
    lines.append(f"*Client-wise Breakdown*")
    lines.append("||Source||Accepted||Rejected||Grand Total||Approval Rate||Rejection Rate||")
    for c in client_data:
        lines.append(
            f"|{c['Source']}|{c['Accepted']}|{c['Rejected']}|{c['Grand Total']}"
            f"|{c['Approval Rate']}|{c['Rejection Rate']}|"
        )
    lines.append("")

    lines.append(f"*Snapshot ({display_date})*")
    lines.append(f"Approved: {accepted}")
    lines.append(f"Rejected: {rejected}")
    lines.append(f"Pending: {pending_count}")
    lines.append("")

    if rejection_reasons:
        lines.append(f"*Rejection Reasons ({display_date})*")
        lines.append("||Reason||Count||")
        for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1]):
            lines.append(f"|{reason}|{count}|")
    else:
        lines.append(f"*Rejection Reasons ({display_date})*")
        lines.append("No rejections on this date.")

    jira_text = "\n".join(lines)
    return jira_text, client_data


# ── Tab: Tracker Updater ─────────────────────────────────────────────────
def tab_tracker_updater():
    st.header("Upload & Deduplicate")
    st.markdown("Upload your files, deduplicate, and download the updated tracker.")

    # ── File Upload ───────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("1. Daily DCR Report")
        st.caption("From Veeva Network (CSV or ZIP)")
        daily_file = st.file_uploader(
            "Upload daily report",
            type=["csv", "zip"],
            key="daily",
            label_visibility="collapsed",
        )

    with col2:
        st.subheader("2. Master Tracker")
        st.caption("Exported 'All Raw' tab (CSV or XLSX)")
        tracker_file = st.file_uploader(
            "Upload master tracker",
            type=["csv", "xlsx"],
            key="tracker",
            label_visibility="collapsed",
        )

    # ── Preview uploaded files ────────────────────────────────────────
    if daily_file or tracker_file:
        st.divider()

    preview_col1, preview_col2 = st.columns(2)

    daily_rows = None
    daily_header = None
    tracker_rows = None
    tracker_header = None

    if daily_file:
        with preview_col1:
            if daily_file.name.endswith(".zip"):
                all_rows = read_uploaded_zip(daily_file)
                daily_file.seek(0)
            else:
                all_rows = read_uploaded_csv(daily_file)
                daily_file.seek(0)

            if all_rows and len(all_rows) > 1:
                daily_header = all_rows[0]
                daily_rows = all_rows[1:]
                st.success(f"Daily report: **{len(daily_rows):,}** rows, **{len(daily_header)}** columns")
                with st.expander("Preview daily report (first 5 rows)"):
                    preview_df = pd.DataFrame(daily_rows[:5], columns=dedupe_columns(daily_header))
                    st.dataframe(preview_df, use_container_width=True)
            else:
                st.error("Could not read the daily report file.")

    if tracker_file:
        with preview_col2:
            if tracker_file.name.endswith(".xlsx"):
                try:
                    tracker_file.seek(0)
                    tracker_df = pd.read_excel(tracker_file, sheet_name="All Raw", dtype=str)
                    tracker_file.seek(0)
                    # Keep only the first 24 tracker columns, drop blank Excel padding
                    tracker_df = tracker_df.iloc[:, :24]
                    # Convert DataFrame to header + rows for dedup logic
                    tracker_header = list(tracker_df.columns)
                    tracker_rows = tracker_df.fillna("").values.tolist()
                    st.success(f"Master tracker: **{len(tracker_rows):,}** rows, **{len(tracker_header)}** columns")
                    with st.expander("Preview master tracker (first 5 rows)"):
                        preview_df = pd.DataFrame(tracker_rows[:5], columns=dedupe_columns(tracker_header))
                        st.dataframe(preview_df, use_container_width=True)
                except Exception as e:
                    st.error(f"Error reading Excel file: {e}")
            else:
                all_rows = read_uploaded_csv(tracker_file)
                tracker_file.seek(0)

                if all_rows and len(all_rows) > 1:
                    tracker_header = all_rows[0]
                    tracker_rows = all_rows[1:]
                    st.success(f"Master tracker: **{len(tracker_rows):,}** rows, **{len(tracker_header)}** columns")
                    with st.expander("Preview master tracker (first 5 rows)"):
                        preview_df = pd.DataFrame(tracker_rows[:5], columns=dedupe_columns(tracker_header))
                        st.dataframe(preview_df, use_container_width=True)
                else:
                    st.error("Could not read the master tracker file.")

    # ── Process Button ────────────────────────────────────────────────
    st.divider()

    if daily_rows and tracker_rows:
        if st.button("🚀 Process & Deduplicate", type="primary", use_container_width=True):
            with st.spinner("Processing..."):
                header_map = {}
                for i, col in enumerate(daily_header):
                    header_map[col] = i

                existing_cr_ids = set()
                for row in tracker_rows:
                    if len(row) > 6:
                        cr_id = row[6].strip()
                        if cr_id:
                            existing_cr_ids.add(cr_id)

                cr_id_col = header_map.get(
                    "change_request.change_request_id (CHANGE REQUEST ID)"
                )

                if cr_id_col is None:
                    st.error("Could not find 'change_request.change_request_id (CHANGE REQUEST ID)' column in daily report.")
                    st.stop()

                new_rows = []
                dupes = 0
                for row in daily_rows:
                    cr_id = row[cr_id_col].strip() if cr_id_col < len(row) else ""
                    if cr_id in existing_cr_ids:
                        dupes += 1
                    else:
                        new_rows.append(transform_daily_row(header_map, row))
                        existing_cr_ids.add(cr_id)

                st.session_state["new_rows"] = new_rows
                st.session_state["dupes"] = dupes
                st.session_state["tracker_header"] = tracker_header
                st.session_state["tracker_rows"] = tracker_rows
                st.session_state["daily_total"] = len(daily_rows)

    # ── Results ───────────────────────────────────────────────────────
    if "new_rows" in st.session_state:
        new_rows = st.session_state["new_rows"]
        dupes = st.session_state["dupes"]
        tracker_header = st.session_state["tracker_header"]
        tracker_rows = st.session_state["tracker_rows"]
        daily_total = st.session_state["daily_total"]

        st.divider()
        st.subheader("Results")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Daily Report Rows", f"{daily_total:,}")
        m2.metric("Duplicates Skipped", f"{dupes:,}")
        m3.metric("New Rows Added", f"{len(new_rows):,}")
        m4.metric("Updated Total", f"{len(tracker_rows) + len(new_rows):,}")

        if len(new_rows) == 0:
            st.info("No new rows to add. Your master tracker is already up to date!")
        else:
            with st.expander(f"Preview new rows ({len(new_rows):,} rows)", expanded=True):
                cols = tracker_header if len(tracker_header) == len(new_rows[0]) else TRACKER_COLUMNS
                preview_df = pd.DataFrame(
                    new_rows[:20],
                    columns=dedupe_columns(cols),
                )
                st.dataframe(preview_df, use_container_width=True)
                if len(new_rows) > 20:
                    st.caption(f"Showing first 20 of {len(new_rows):,} new rows")

            combined_rows = tracker_rows + new_rows
            csv_bytes = rows_to_csv_bytes(tracker_header, combined_rows)

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            filename = f"Customer DCR Tracker - All Raw - Updated_{timestamp}.csv"

            st.download_button(
                label=f"📥 Download Updated Tracker ({len(combined_rows):,} rows)",
                data=csv_bytes,
                file_name=filename,
                mime="text/csv",
                type="primary",
                use_container_width=True,
            )

            st.caption("Upload this CSV back to the 'All Raw' tab in Google Sheets.")
    elif daily_rows is None or tracker_rows is None:
        st.info("Upload both files above to get started.")


# ── Sprint JIRA Content Generator ────────────────────────────────────────
def read_existing_pivot(excel_file):
    """Read existing JIRA Pivot and Daily DCR Update data from the Excel tracker."""
    try:
        excel_file.seek(0)
        xls = pd.ExcelFile(excel_file)
        pivot_data = {}

        # Read "Daily DCR Update" for existing cumulative monthly data
        if "Daily DCR Update" in xls.sheet_names:
            dcu = pd.read_excel(excel_file, sheet_name="Daily DCR Update", header=None)
            excel_file.seek(0)

            # Parse the title to get the month
            title_cell = dcu.iloc[0, 0] if not dcu.empty else ""
            pivot_data["title"] = str(title_cell) if title_cell and not pd.isna(title_cell) else ""

            # Parse client breakdown (rows 4-12, cols B-I in the sheet → 0-indexed rows 3-11, cols 1-8)
            # Header row (row index 3): Pfizer, %, Takeda, %, Sanofi, %, Total, %
            clients = ["Pfizer", "Takeda", "Sanofi"]
            client_cols = {
                "Pfizer": 1,   # col B
                "Takeda": 3,   # col D
                "Sanofi": 5,   # col F
            }
            row_labels = {
                4: "Add REQ._Accepted",
                5: "Change REQ._Accepted",
                6: "Accepted",
                7: "Add REQ._Rejected",
                8: "Change REQ._Rejected",
                9: "Rejected",
                10: "Pending",
                11: "Total",
                12: "Resolution Time",
            }

            existing_client_data = {}
            for client, col_idx in client_cols.items():
                client_vals = {}
                for row_idx, label in row_labels.items():
                    val = dcu.iloc[row_idx, col_idx] if row_idx < len(dcu) and col_idx < dcu.shape[1] else None
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        client_vals[label] = val
                    else:
                        client_vals[label] = 0
                existing_client_data[client] = client_vals

            # Also get the Total column
            total_vals = {}
            total_col = 7  # col H
            for row_idx, label in row_labels.items():
                val = dcu.iloc[row_idx, total_col] if row_idx < len(dcu) and total_col < dcu.shape[1] else None
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    total_vals[label] = val
                else:
                    total_vals[label] = 0
            existing_client_data["Total"] = total_vals

            pivot_data["client_data"] = existing_client_data

        # Read "JIRA Pivot" for overall summary
        if "JIRA Pivot" in xls.sheet_names:
            jp = pd.read_excel(excel_file, sheet_name="JIRA Pivot", header=None)
            excel_file.seek(0)
            # Parse known positions: I1=Approved, J1=count, I2=Rejected, J2=count, etc.
            summary = {}
            for _, row in jp.iterrows():
                for col_idx in range(jp.shape[1]):
                    val = row.iloc[col_idx]
                    if isinstance(val, str):
                        # Check next column for the value
                        if col_idx + 1 < jp.shape[1]:
                            next_val = row.iloc[col_idx + 1]
                            if not (isinstance(next_val, float) and pd.isna(next_val)):
                                summary[val] = next_val
            pivot_data["summary"] = summary

        return pivot_data
    except Exception as e:
        return {"error": str(e)}


def generate_sprint_jira_content(df, start_date, end_date, pending_count, existing_data=None):
    """Generate JIRA-formatted sprint update with existing pivot data factored in."""
    if start_date == end_date:
        display_date = start_date.strftime("%d-%b-%Y")
    else:
        display_date = f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}"

    # Filter rows completed within the sprint range
    def date_in_range(completed_str):
        try:
            d = str(completed_str)[:10]
            return start_date.strftime("%Y-%m-%d") <= d <= end_date.strftime("%Y-%m-%d")
        except (TypeError, IndexError):
            return False

    sprint_df = df[df["COMPLETED DATE"].apply(date_in_range)].copy()

    if sprint_df.empty:
        return None, f"No data found for {display_date}", None

    # Apply source replacements
    sprint_df["Source_Display"] = sprint_df["change_request.source (SOURCE)"].apply(replace_source_name)

    # ── Compute sprint metrics per client ─────────────────────────────
    clients_order = ["Pfizer", "Takeda", "Sanofi"]
    # Map sources to client names
    source_to_client = {
        "Pfizer": "Pfizer",
        "OpenData API": "Sanofi",
        "Sanofi": "Sanofi",
        "Veeva CRM": "Takeda",
        "Takeda": "Takeda",
    }

    sprint_client_data = {}
    for client in clients_order:
        sprint_client_data[client] = {
            "Add REQ._Accepted": 0,
            "Change REQ._Accepted": 0,
            "Accepted": 0,
            "Add REQ._Rejected": 0,
            "Change REQ._Rejected": 0,
            "Rejected": 0,
            "Pending": 0,
            "Total": 0,
            "Resolution Time": 0,
        }

    for _, row in sprint_df.iterrows():
        source = row.get("Source_Display", "")
        client = source_to_client.get(source, source)
        if client not in sprint_client_data:
            sprint_client_data[client] = {
                "Add REQ._Accepted": 0, "Change REQ._Accepted": 0, "Accepted": 0,
                "Add REQ._Rejected": 0, "Change REQ._Rejected": 0, "Rejected": 0,
                "Pending": 0, "Total": 0, "Resolution Time": 0,
            }
            clients_order.append(client)

        resolution = row.get("RESOLUTION", "")
        cr_type = str(row.get("change_request.change_request_type (TYPE)", "")).strip().upper()

        is_add = "ADD" in cr_type

        if resolution == "Accepted":
            sprint_client_data[client]["Accepted"] += 1
            if is_add:
                sprint_client_data[client]["Add REQ._Accepted"] += 1
            else:
                sprint_client_data[client]["Change REQ._Accepted"] += 1
        elif resolution == "Rejected":
            sprint_client_data[client]["Rejected"] += 1
            if is_add:
                sprint_client_data[client]["Add REQ._Rejected"] += 1
            else:
                sprint_client_data[client]["Change REQ._Rejected"] += 1

        sprint_client_data[client]["Total"] += 1

    # Compute sprint SLA per client (weekdays only)
    sprint_sla_by_client = {}
    for client in clients_order:
        client_sources = [s for s, c in source_to_client.items() if c == client]
        client_mask = sprint_df["Source_Display"].isin([client] + client_sources)
        client_df = sprint_df[client_mask]
        weekday_mask = client_df["COMPLETED DATE"].apply(is_weekday)
        weekday_df = client_df[weekday_mask]
        sla_vals = weekday_df["Service Time"].apply(parse_service_time_hours).dropna()
        if len(sla_vals) > 0:
            avg_h = sla_vals.mean()
            sprint_sla_by_client[client] = avg_h
        else:
            sprint_sla_by_client[client] = None

    # ── Overall sprint summary ────────────────────────────────────────
    total = len(sprint_df)
    accepted = len(sprint_df[sprint_df["RESOLUTION"] == "Accepted"])
    rejected = len(sprint_df[sprint_df["RESOLUTION"] == "Rejected"])
    acc_pct = (accepted / total * 100) if total > 0 else 0
    rej_pct = (rejected / total * 100) if total > 0 else 0

    weekday_mask = sprint_df["COMPLETED DATE"].apply(is_weekday)
    weekday_df = sprint_df[weekday_mask]
    sla_hours = weekday_df["Service Time"].apply(parse_service_time_hours).dropna()
    missed_sla = int((sla_hours > 24).sum()) if len(sla_hours) > 0 else 0
    avg_sla = sla_hours.mean() if len(sla_hours) > 0 else 0

    # ── Combine with existing data if available ───────────────────────
    combined_client_data = {}
    if existing_data and "client_data" in existing_data:
        ex = existing_data["client_data"]
        for client in clients_order:
            ex_client = ex.get(client, {})
            sp_client = sprint_client_data.get(client, {})
            combined = {}
            for key in ["Add REQ._Accepted", "Change REQ._Accepted", "Accepted",
                        "Add REQ._Rejected", "Change REQ._Rejected", "Rejected",
                        "Pending", "Total"]:
                ex_val = ex_client.get(key, 0)
                sp_val = sp_client.get(key, 0)
                # Convert to int safely
                try:
                    ex_val = int(float(ex_val)) if ex_val else 0
                except (ValueError, TypeError):
                    ex_val = 0
                combined[key] = ex_val + sp_val
            combined_client_data[client] = combined
    else:
        combined_client_data = sprint_client_data

    # Compute combined totals
    combined_totals = {}
    for key in ["Add REQ._Accepted", "Change REQ._Accepted", "Accepted",
                "Add REQ._Rejected", "Change REQ._Rejected", "Rejected",
                "Pending", "Total"]:
        combined_totals[key] = sum(combined_client_data.get(c, {}).get(key, 0) for c in clients_order)

    # ── Rejection Reasons ─────────────────────────────────────────────
    rej_df = sprint_df[sprint_df["RESOLUTION"] == "Rejected"]
    rejection_reasons = {}
    if len(rej_df) > 0:
        reason_counts = rej_df["Reason for Rejection"].value_counts()
        for reason, count in reason_counts.items():
            if reason and reason != "N/A" and reason != "#N/A":
                rejection_reasons[reason] = int(count)

    # ── Build JIRA Text ───────────────────────────────────────────────
    lines = []
    lines.append(f"*Overall Summary ({display_date})*")
    lines.append(f"Total DCRs Processed: {total}")
    lines.append(f"Approved: {accepted} ({acc_pct:.1f}%)")
    lines.append(f"Rejected: {rejected} ({rej_pct:.1f}%)")
    lines.append(f"Missed SLA: {missed_sla}")
    lines.append(f"Average SLA: {format_hours_hhmm(avg_sla)}")
    lines.append("")

    # Client breakdown table (matching Daily DCR Update format)
    lines.append(f"*Client-wise Breakdown*")

    # Build header
    header_parts = ["||Category"]
    for c in clients_order:
        header_parts.append(f"||{c}||%")
    header_parts.append("||Total||%||")
    lines.append("".join(header_parts))

    # Rows: Add REQ. Accepted, Change REQ. Accepted, Accepted total, Add REQ. Rejected, Change REQ. Rejected, Rejected total, Pending, Total
    row_keys = [
        ("Add REQ._Accepted", "Add REQ. Accepted"),
        ("Change REQ._Accepted", "Change REQ. Accepted"),
        ("Accepted", "Accepted"),
        ("Add REQ._Rejected", "Add REQ. Rejected"),
        ("Change REQ._Rejected", "Change REQ. Rejected"),
        ("Rejected", "Rejected"),
        ("Pending", "Pending"),
        ("Total", "Total"),
    ]

    for key, label in row_keys:
        parts = [f"|{label}"]
        for c in clients_order:
            val = combined_client_data.get(c, {}).get(key, 0)
            c_total = combined_client_data.get(c, {}).get("Total", 0)
            pct = f"{val / c_total * 100:.0f}%" if c_total > 0 and key != "Total" else ""
            if key == "Pending":
                pct = ""
            parts.append(f"|{val}|{pct}")
        # Total column
        t_val = combined_totals.get(key, 0)
        grand_total = combined_totals.get("Total", 0)
        t_pct = f"{t_val / grand_total * 100:.0f}%" if grand_total > 0 and key != "Total" else ""
        if key == "Pending":
            t_pct = ""
        if key == "Total":
            t_pct = "100%"
        parts.append(f"|{t_val}|{t_pct}|")
        lines.append("".join(parts))

    # Resolution time row
    sla_parts = ["|Resolution Time"]
    for c in clients_order:
        sla_val = sprint_sla_by_client.get(c)
        sla_parts.append(f"|{format_hours_hhmm(sla_val) if sla_val else '-'}|")
    sla_parts.append(f"|{format_hours_hhmm(avg_sla)}||")
    lines.append("".join(sla_parts))

    lines.append("")

    lines.append(f"*Snapshot ({display_date})*")
    lines.append(f"Approved: {accepted}")
    lines.append(f"Rejected: {rejected}")
    lines.append(f"Pending: {pending_count}")
    lines.append("")

    if rejection_reasons:
        lines.append(f"*Rejection Reasons ({display_date})*")
        lines.append("||Reason||Count||")
        for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1]):
            lines.append(f"|{reason}|{count}|")
    else:
        lines.append(f"*Rejection Reasons ({display_date})*")
        lines.append("No rejections in this sprint.")

    jira_text = "\n".join(lines)

    # Build visual summary data
    visual_data = {
        "sprint_client_data": sprint_client_data,
        "combined_client_data": combined_client_data,
        "combined_totals": combined_totals,
        "clients_order": clients_order,
        "total": total,
        "accepted": accepted,
        "rejected": rejected,
        "missed_sla": missed_sla,
        "avg_sla": avg_sla,
    }

    return jira_text, visual_data, rejection_reasons


def tab_jira_generator():
    st.header("JIRA Sprint Update Generator")
    st.markdown("Upload the tracker (.xlsx or .csv), select sprint dates, and get ready-to-paste JIRA content.")

    # Single file upload
    jira_tracker = st.file_uploader(
        "Upload Master Tracker (.xlsx with pivot data, or .csv 'All Raw' export)",
        type=["csv", "xlsx"],
        key="jira_tracker",
    )

    if not jira_tracker:
        st.info("Upload the master tracker to generate JIRA content.")
        return

    # Read tracker data + auto-detect pivot from xlsx
    existing_data = None
    if jira_tracker.name.endswith(".xlsx"):
        try:
            jira_tracker.seek(0)
            df = pd.read_excel(jira_tracker, sheet_name="All Raw")
            jira_tracker.seek(0)
            df.columns = dedupe_columns(list(df.columns))
            # Auto-load existing pivot data from same file
            existing_data = read_existing_pivot(jira_tracker)
            if "error" in existing_data:
                existing_data = None
        except Exception as e:
            st.error(f"Error reading Excel file: {e}")
            return
    else:
        all_rows = read_uploaded_csv(jira_tracker)
        jira_tracker.seek(0)
        if not all_rows or len(all_rows) < 2:
            st.error("Could not read the tracker file.")
            return
        header = all_rows[0]
        data_rows = all_rows[1:]
        df = pd.DataFrame(data_rows, columns=dedupe_columns(header))

    msg = f"Tracker loaded: **{len(df):,}** rows"
    if existing_data and "client_data" in existing_data:
        title = existing_data.get("title", "")
        msg += f" | Existing pivot data detected ({title})" if title else " | Existing pivot data detected"
    st.success(msg)

    # Ensure COMPLETED DATE column is string for filtering
    if "COMPLETED DATE" in df.columns:
        df["COMPLETED DATE"] = df["COMPLETED DATE"].astype(str)

    # Find available dates
    completed_dates = df["COMPLETED DATE"].dropna().apply(
        lambda x: str(x)[:10] if len(str(x)) >= 10 else ""
    )
    valid_dates = sorted(set(d for d in completed_dates if d and d != "" and d != "nan" and d != "NaT"), reverse=True)

    if not valid_dates:
        st.error("No valid completed dates found in the tracker.")
        return

    min_date = datetime.strptime(valid_dates[-1], "%Y-%m-%d").date()
    max_date = datetime.strptime(valid_dates[0], "%Y-%m-%d").date()

    st.divider()

    col_date, col_pending = st.columns([2, 1])

    with col_date:
        date_range = st.date_input(
            "Select sprint date range",
            value=(max_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="jira_date",
            help="Select start and end dates for the sprint (e.g., 2-week sprint, or Fri–Sun for Monday updates)",
        )

    # Handle single date vs range
    if isinstance(date_range, (list, tuple)):
        if len(date_range) == 2:
            start_date, end_date = date_range
        elif len(date_range) == 1:
            start_date = end_date = date_range[0]
        else:
            st.warning("Please select a date or date range.")
            return
    else:
        start_date = end_date = date_range

    with col_pending:
        pending_count = st.number_input(
            "Pending DCRs (manual entry)",
            min_value=0,
            value=0,
            step=1,
            key="pending_count",
            help="Enter the current pending count from the live dashboard",
        )

    # Quick stats for the selected range
    def date_in_range_check(completed_str):
        try:
            d = str(completed_str)[:10]
            return start_date.strftime("%Y-%m-%d") <= d <= end_date.strftime("%Y-%m-%d")
        except (TypeError, IndexError):
            return False

    day_count = len(df[df["COMPLETED DATE"].apply(date_in_range_check)])

    if start_date == end_date:
        display_label = start_date.strftime('%d-%b-%Y')
    else:
        display_label = f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}"

    if day_count == 0:
        st.warning(f"No DCRs completed on {display_label}. Try a different date range.")
        nearby = [d for d in valid_dates if abs((datetime.strptime(d, "%Y-%m-%d").date() - start_date).days) <= 7]
        if nearby:
            st.caption(f"Nearby dates with data: {', '.join(nearby[:5])}")
        return

    st.info(f"**{day_count}** DCRs completed in sprint: {display_label}")

    if st.button("Generate JIRA Content", type="primary", use_container_width=True):
        with st.spinner("Generating..."):
            jira_text, visual_data, rejection_reasons = generate_sprint_jira_content(
                df, start_date, end_date, pending_count, existing_data
            )

        if jira_text is None:
            st.error(visual_data)
            return

        st.divider()
        st.subheader(f"Sprint Update — {display_label}")

        # Visual summary metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Processed", visual_data["total"])
        m2.metric("Approved", visual_data["accepted"])
        m3.metric("Rejected", visual_data["rejected"])
        m4.metric("Pending", pending_count)
        m5.metric("Missed SLA", visual_data["missed_sla"])

        # Combined client breakdown table
        clients = visual_data["clients_order"]
        st.subheader("Client-wise Breakdown")
        combined_rows = []
        for key, label in [
            ("Add REQ._Accepted", "Add REQ. Accepted"),
            ("Change REQ._Accepted", "Change REQ. Accepted"),
            ("Accepted", "Accepted"),
            ("Add REQ._Rejected", "Add REQ. Rejected"),
            ("Change REQ._Rejected", "Change REQ. Rejected"),
            ("Rejected", "Rejected"),
            ("Total", "Total"),
        ]:
            row_data = {"Category": label}
            for c in clients:
                row_data[c] = visual_data["combined_client_data"].get(c, {}).get(key, 0)
            row_data["Grand Total"] = visual_data["combined_totals"].get(key, 0)
            combined_rows.append(row_data)

        combined_table_df = pd.DataFrame(combined_rows)
        st.dataframe(combined_table_df, use_container_width=True, hide_index=True)

        # JIRA formatted output
        st.divider()
        st.subheader("Copy-Paste JIRA Content")
        st.code(jira_text, language=None)
        st.caption("Click the copy icon (top-right of the code block) to copy to clipboard.")


# ── Main App ─────────────────────────────────────────────────────────────
def main():
    st.title("📊 DCR Tracker Tool")

    # Sidebar
    with st.sidebar:
        st.header("About")
        st.markdown("""
        **Tracker Updater** — Dedup and append new DCR data to the master tracker.

        **JIRA Generator** — Generate sprint JIRA update content from tracker data.

        ---

        **Source Replacements:**
        - OpenData API → Sanofi
        - Veeva CRM → Takeda

        **SLA Threshold:** 24 hours
        """)

    # Tabs
    tab1, tab2 = st.tabs(["📥 Tracker Updater", "📋 JIRA Generator"])

    with tab1:
        tab_tracker_updater()

    with tab2:
        tab_jira_generator()


if __name__ == "__main__":
    main()

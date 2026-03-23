#!/usr/bin/env python3
"""
DCR Master Tracker Updater
--------------------------
Automates: Download CSV (from Veeva zip) → Dedup against master tracker → Append new rows

Usage:
    python3 dcr_updater.py

It will:
1. Find the latest Veeva DCR zip/csv in ~/Downloads
2. Load the master tracker CSV (exported from Google Sheets "All Raw" tab)
3. Deduplicate on Change Request ID
4. Compute derived columns (Service Time, Month, Resolution Note Code, etc.)
5. Append new rows and save an updated CSV ready to re-upload
"""

import csv
import glob
import os
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────
DOWNLOADS = Path.home() / "Downloads"
DAILY_PATTERN = "Customer DCRs with Detailed Information - IND - Updated*"
TRACKER_PATTERN = "Customer DCR Tracker & Dashboards_IN OD*All Raw*.csv"

# Resolution Note Code → Reason for Rejection mapping
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

# Master tracker column order
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
    "CREATED DATE",  # duplicate col 16 — same value as col 1
    "COMPLETED DATE",  # duplicate col 17 — same value as col 2
    "change_request.resolution (RESOLUTION)",
    "RESOLUTION",
    "Resolution Note Code",
    "Reason for Rejection",
    "Resolution Note_Trimmed",
    "change_request.resolution_notes (RESOLUTION NOTES)",
    "Month",
]

# Since CSV can't have duplicate column names cleanly, we use positional indexing
# for the output. We'll handle this with a list-based approach.


def find_latest_file(directory, pattern):
    """Find the most recent file matching the glob pattern."""
    matches = sorted(directory.glob(pattern), key=os.path.getmtime, reverse=True)
    return matches[0] if matches else None


def find_daily_report():
    """Find and extract the latest Veeva DCR daily report CSV."""
    # Try zip first (they come as zips from Veeva)
    zip_file = find_latest_file(DOWNLOADS, DAILY_PATTERN + ".zip")
    csv_file = find_latest_file(DOWNLOADS, DAILY_PATTERN + ".csv")

    if zip_file and csv_file:
        # Use whichever is newer
        if os.path.getmtime(zip_file) > os.path.getmtime(csv_file):
            source = zip_file
        else:
            return csv_file, csv_file.name
    elif zip_file:
        source = zip_file
    elif csv_file:
        return csv_file, csv_file.name
    else:
        return None, None

    # Extract CSV from zip
    if source.suffix == ".zip":
        with zipfile.ZipFile(source, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                print(f"  ERROR: No CSV found inside {source.name}")
                return None, None
            extract_path = DOWNLOADS / csv_names[0]
            if not extract_path.exists():
                zf.extract(csv_names[0], DOWNLOADS)
            return extract_path, source.name
    return source, source.name


def find_tracker():
    """Find the master tracker CSV (exported from Google Sheets)."""
    return find_latest_file(DOWNLOADS, TRACKER_PATTERN)


def compute_service_time(created_str, completed_str):
    """Calculate service time as H:MM:SS between created and completed dates."""
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        created = datetime.strptime(created_str.strip(), fmt)
        completed = datetime.strptime(completed_str.strip(), fmt)
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
    """Extract YYYY-MM from the created date."""
    try:
        return created_str.strip()[:7]
    except (AttributeError, IndexError):
        return ""


def extract_resolution_note_code(resolution_notes):
    """Extract the last [X-NNNNN] code from resolution notes."""
    codes = re.findall(r"\[[A-Z]-\d{5}\]", resolution_notes or "")
    return codes[-1] if codes else ""


def extract_resolution_note_trimmed(resolution_notes, code):
    """Extract text from the last resolution code onwards."""
    if not code or not resolution_notes:
        return ""
    idx = resolution_notes.rfind(code)
    if idx >= 0:
        return resolution_notes[idx:]
    return ""


def lookup_rejection_reason(code):
    """Map a resolution note code to its rejection reason."""
    return CODE_TO_REASON.get(code, "N/A")


def transform_daily_row(row):
    """Transform a daily report row into tracker format (as a list of values)."""
    vid = row.get("change_request_hcp.vid__v (VID)", "")
    cr_id = row.get("change_request.change_request_id (CHANGE REQUEST ID)", "")
    subject = row.get("change_request.subject (SUBJECT)", "")
    hcp_type = row.get("TYPE", "")
    specialty = row.get("SPECIALTY", "")
    source = row.get("change_request.source (SOURCE)", "")
    orig_sys = row.get("change_request.originating_system (ORIGINATING SYSTEM)", "")
    created_by = row.get("change_request.created_by (CREATED BY)", "")
    cr_type = row.get("change_request.change_request_type (TYPE)", "")
    owner = row.get("change_request.owner (OWNER)", "")
    created_date = row.get("CREATED DATE", "")
    completed_date = row.get("COMPLETED DATE", "")
    resolution = row.get("RESOLUTION", "")
    resolution_notes = row.get("change_request.resolution_notes (RESOLUTION NOTES)", "")

    # Derived columns
    service_time = compute_service_time(created_date, completed_date)
    month = compute_month(created_date)
    note_code = extract_resolution_note_code(resolution_notes)
    note_trimmed = extract_resolution_note_trimmed(resolution_notes, note_code)
    rejection_reason = lookup_rejection_reason(note_code)

    # Build row in tracker column order (24 columns)
    return [
        created_date,       # Col 1: CREATED DATE
        completed_date,     # Col 2: COMPLETED DATE
        service_time,       # Col 3: Service Time
        vid,                # Col 4: Entity Link (= VID)
        cr_id,              # Col 5: Task Link (= Change Request ID)
        vid,                # Col 6: hcp.vid__v (VID)
        cr_id,              # Col 7: change_request.change_request_id
        subject,            # Col 8: change_request.subject
        hcp_type,           # Col 9: hcp.type (TYPE)
        specialty,          # Col 10: hcp.specialty (SPECIALTY)
        source,             # Col 11: change_request.source
        orig_sys,           # Col 12: change_request.originating_system
        created_by,         # Col 13: change_request.created_by
        cr_type,            # Col 14: change_request.change_request_type
        owner,              # Col 15: change_request.owner
        created_date,       # Col 16: CREATED DATE (duplicate)
        completed_date,     # Col 17: COMPLETED DATE (duplicate)
        resolution,         # Col 18: change_request.resolution (RESOLUTION)
        resolution,         # Col 19: RESOLUTION
        note_code,          # Col 20: Resolution Note Code
        rejection_reason,   # Col 21: Reason for Rejection
        note_trimmed,       # Col 22: Resolution Note_Trimmed
        resolution_notes,   # Col 23: change_request.resolution_notes
        month,              # Col 24: Month
    ]


def main():
    print("=" * 60)
    print("  DCR Master Tracker Updater")
    print("=" * 60)

    # Step 1: Find files
    print("\n[1/4] Finding files...")
    daily_path, daily_name = find_daily_report()
    if not daily_path:
        print("  ERROR: No daily DCR report found in Downloads.")
        print(f"  Looking for: {DAILY_PATTERN}.zip or .csv")
        sys.exit(1)
    print(f"  Daily report: {daily_name}")

    tracker_path = find_tracker()
    if not tracker_path:
        print("  ERROR: No master tracker CSV found in Downloads.")
        print(f"  Looking for: {TRACKER_PATTERN}")
        print("  Please export the 'All Raw' tab from Google Sheets as CSV first.")
        sys.exit(1)
    print(f"  Master tracker: {tracker_path.name}")

    # Step 2: Load existing Change Request IDs from tracker
    print("\n[2/4] Loading master tracker...")
    existing_cr_ids = set()
    tracker_rows = []
    tracker_header = None

    with open(tracker_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        tracker_header = next(reader)
        for row in reader:
            tracker_rows.append(row)
            # Change Request ID is column index 6 (0-based)
            if len(row) > 6:
                cr_id = row[6].strip()
                if cr_id:
                    existing_cr_ids.add(cr_id)

    print(f"  Existing rows: {len(tracker_rows):,}")
    print(f"  Unique Change Request IDs: {len(existing_cr_ids):,}")

    # Step 3: Load daily report and find new rows
    print("\n[3/4] Processing daily report...")
    new_rows = []
    total_daily = 0
    dupes = 0

    with open(daily_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_daily += 1
            cr_id = row.get(
                "change_request.change_request_id (CHANGE REQUEST ID)", ""
            ).strip()
            if cr_id in existing_cr_ids:
                dupes += 1
            else:
                new_rows.append(transform_daily_row(row))
                existing_cr_ids.add(cr_id)  # prevent dupes within daily file too

    print(f"  Daily report rows: {total_daily:,}")
    print(f"  Duplicates skipped: {dupes:,}")
    print(f"  New rows to add: {len(new_rows):,}")

    if not new_rows:
        print("\n  No new rows to add. Master tracker is already up to date!")
        sys.exit(0)

    # Step 4: Write updated tracker
    print("\n[4/4] Saving updated tracker...")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    output_name = f"Customer DCR Tracker - All Raw - Updated_{timestamp}.csv"
    output_path = DOWNLOADS / output_name

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(tracker_header)
        for row in tracker_rows:
            writer.writerow(row)
        for row in new_rows:
            writer.writerow(row)

    final_count = len(tracker_rows) + len(new_rows)
    print(f"  Saved: {output_name}")
    print(f"  Total rows: {final_count:,}")

    print("\n" + "=" * 60)
    print(f"  DONE! {len(new_rows):,} new rows added.")
    print(f"  Output: {output_path}")
    print("  Next: Upload this CSV to the 'All Raw' tab in Google Sheets.")
    print("=" * 60)


if __name__ == "__main__":
    main()

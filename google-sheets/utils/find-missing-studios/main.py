#!/usr/bin/env python3
"""
find_missing_studios.py

Purpose:
--------
Compare studio names between two Google Sheets worksheets and
export studios present in Data18 Studios but missing from Networks.

Features:
---------
✓ Read-only Google Sheets access
✓ Optional case-insensitive comparison
✓ Clean JSON output
"""

import json
from pathlib import Path
from typing import List, Set

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =====================================================================
# CONFIG
# =====================================================================

BASE_DIR = Path(__file__).resolve().parents[3]

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"
DATA_DIR = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "find-missing-studios"
    / "data"
)

DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_JSON_FILE = DATA_DIR / "missing-studios.json"

SPREADSHEET_ID = "1ZBydtcOO3UOOMvKOZ6b_Ze-JDnz6VRbDaqKXkjInkV0"

NETWORKS_SHEET = "Networks"
DATA18_SHEET = "Data18 Studios"

NETWORKS_START_COL = "B"
DATA18_START_COL = "A"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# =====================================================================
# GOOGLE SHEETS AUTH
# =====================================================================

def get_sheets_service():
    """
    Return an authenticated Google Sheets API service.
    """
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)

# =====================================================================
# DATA FETCHING
# =====================================================================

def fetch_sheet_values(sheet_name: str, start_col: str) -> List[str]:
    """
    Fetch all non-empty values from a sheet starting at a given column.

    Reads from row 2 onward and flattens the grid.
    """
    service = get_sheets_service()
    range_name = f"{sheet_name}!{start_col}2:ZZ"

    response = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
        )
        .execute()
    )

    values = response.get("values", [])
    return [
        cell.strip()
        for row in values
        for cell in row
        if isinstance(cell, str) and cell.strip()
    ]

# =====================================================================
# COMPARISON LOGIC
# =====================================================================

def normalize(value: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return " ".join(value.lower().split())

def find_missing_entries(case_insensitive: bool = False) -> List[str]:
    """
    Find studios present in Data18 sheet but missing from Networks sheet.
    """
    networks = fetch_sheet_values(NETWORKS_SHEET, NETWORKS_START_COL)
    data18_studios = fetch_sheet_values(DATA18_SHEET, DATA18_START_COL)

    if case_insensitive:
        networks_set: Set[str] = {normalize(v) for v in networks}
        missing = {
            studio
            for studio in data18_studios
            if normalize(studio) not in networks_set
        }
    else:
        networks_set = set(networks)
        missing = set(data18_studios) - networks_set

    return sorted(missing)

# =====================================================================
# OUTPUT
# =====================================================================

def write_missing_to_json(values: List[str]) -> Path:
    """
    Write missing studio names to JSON file.
    """
    OUTPUT_JSON_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_JSON_FILE.open("w", encoding="utf-8") as f:
        json.dump(values, f, indent=2, ensure_ascii=False)

    print(f"✅ Missing studios written to {OUTPUT_JSON_FILE}")
    return OUTPUT_JSON_FILE

# =====================================================================
# MAIN
# =====================================================================

def main() -> None:
    missing = find_missing_entries(case_insensitive=False)

    if missing:
        write_missing_to_json(missing)
        print(f"⚠️ Found {len(missing)} missing studio(s).")
    else:
        print("✅ All Data18 studios are already present in Networks.")

if __name__ == "__main__":
    main()

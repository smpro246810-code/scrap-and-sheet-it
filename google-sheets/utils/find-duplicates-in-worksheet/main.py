"""
find_duplicates_in_worksheet.py

Purpose:
--------
Scan a Google Sheets worksheet and detect duplicate cell values
(across all rows and columns).

Features:
---------
✓ Optional case-sensitive / insensitive comparison
✓ Ignores blank cells
✓ JSON report output
"""

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List

import gspread
from google.oauth2.service_account import Credentials

# =====================================================================
# CONFIG
# =====================================================================

BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "find-duplicates-in-worksheet"
    / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"
OUTPUT_DUPLICATES_FILE = DATA_DIR / "duplicates-report.json"

SPREADSHEET_NAME = "MY PORN"
WORKSHEET_NAME = "Networks"

CASE_SENSITIVE = False

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# =====================================================================
# GOOGLE SHEETS ACCESS
# =====================================================================

def get_worksheet():
    """
    Authenticate and return the target worksheet.
    """
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=SCOPES,
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open(SPREADSHEET_NAME)
    return spreadsheet.worksheet(WORKSHEET_NAME)


def fetch_all_cells() -> List[str]:
    """
    Fetch all non-empty cell values from the worksheet.
    """
    worksheet = get_worksheet()
    rows = worksheet.get_all_values()

    values: List[str] = []
    for row in rows:
        for cell in row:
            if isinstance(cell, str) and cell.strip():
                value = cell.strip()
                values.append(value if CASE_SENSITIVE else value.lower())

    return values

# =====================================================================
# DUPLICATE DETECTION
# =====================================================================

def find_duplicates() -> Dict[str, int]:
    """
    Find duplicate values and return a mapping of value -> count.
    """
    values = fetch_all_cells()
    counts = Counter(values)

    return {
        value: count
        for value, count in counts.items()
        if count > 1
    }

# =====================================================================
# OUTPUT
# =====================================================================

def write_duplicates_report(duplicates: Dict[str, int]) -> None:
    """
    Write duplicate report to JSON file.
    """
    with OUTPUT_DUPLICATES_FILE.open("w", encoding="utf-8") as f:
        json.dump(duplicates, f, indent=2, ensure_ascii=False)

    print(f"📄 Duplicates saved to: {OUTPUT_DUPLICATES_FILE}")

# =====================================================================
# MAIN
# =====================================================================

def main() -> None:
    duplicates = find_duplicates()

    if not duplicates:
        print(f"✅ No duplicates found in '{WORKSHEET_NAME}'.")
        return

    print(f"🔁 Found {len(duplicates)} duplicate entries in '{WORKSHEET_NAME}':")
    for value, count in duplicates.items():
        print(f"  - '{value}' appears {count} times")

    write_duplicates_report(duplicates)


if __name__ == "__main__":
    main()

"""
Data18 Studios → Google Sheets Uploader
======================================

• Loads processed Data18 studio hierarchy
• Formats grouped studios into wide rows (C → CC)
• Uploads cleanly into a Google Sheet worksheet
• Preserves grouping, ordering, and title formatting rules

"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
from pathlib import Path
from typing import List, Dict, Any

# ============================================================
# THIRD-PARTY
# ============================================================

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# PATHS & CONSTANTS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

FIXED_DATA18_STUDIOS_FILE = (
    BASE_DIR
    / "scrapers"
    / "data18"
    / "utils"
    / "fix-data18-studios-hierarchy"
    / "data"
    / "fixed-data18-studios.json"
)

SPREADSHEET_NAME = "MY PORN"
WORKSHEET_NAME = "Data18 Studios"

COLUMN_START = 3  # Column C
COLUMN_END = 80  # Column CC
MAX_SITES_PER_ROW = COLUMN_END - COLUMN_START + 1

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ============================================================
# GOOGLE SHEETS AUTH
# ============================================================


def get_worksheet():
    """
    Authenticate and return the target worksheet.
    Creates the worksheet if missing.
    """
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=SCOPES,
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open(SPREADSHEET_NAME)

    try:
        return spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=WORKSHEET_NAME,
            rows=2000,
            cols=100,
        )


# ============================================================
# DATA LOADING
# ============================================================


def load_processed_studios() -> List[Dict[str, Any]]:
    """
    Load and order processed Data18 studios.

    • Normal groups sorted alphabetically
    • 'Others' group always last
    """
    if not FIXED_DATA18_STUDIOS_FILE.exists():
        raise FileNotFoundError(
            f"Processed studios file not found: {FIXED_DATA18_STUDIOS_FILE}"
        )

    with open(FIXED_DATA18_STUDIOS_FILE, "r", encoding="utf-8") as f:
        studios = json.load(f)

    others = [s for s in studios if s.get("title") == "Others"]
    normal = [s for s in studios if s.get("title") != "Others"]

    normal.sort(key=lambda x: (x.get("title") or "").lower())

    return normal + others


# ============================================================
# FORMATTING HELPERS
# ============================================================


def to_title_case(value: str) -> str:
    """
    Smart title casing for Data18 names.

    Rules preserved:
    • Removes '?'
    • Keeps digits untouched
    • Keeps ALL CAPS untouched
    • Keeps mixed-case untouched
    • Only title-cases fully lower/upper words
    """
    if not value or not isinstance(value, str):
        return value

    value = value.replace("?", "")
    words = value.split()
    formatted = []

    for word in words:
        if any(c.isdigit() for c in word):
            formatted.append(word)
        elif word.isupper():
            formatted.append(word)
        elif not (word.islower() or word.isupper()):
            formatted.append(word)
        else:
            formatted.append(word.title())

    return " ".join(formatted)


# ============================================================
# ROW BUILDER
# ============================================================


def build_rows(studios: List[Dict[str, Any]]) -> List[List[str]]:
    """
    Convert grouped studios into worksheet rows.

    Layout:
    [Group Title, "", Site 1, Site 2, ... Site N]
    """
    rows: List[List[str]] = []

    for studio in studios:
        group_name = to_title_case((studio.get("title") or "").strip())
        sites = [
            to_title_case((s.get("title") or "").strip())
            for s in studio.get("sites", [])
        ]

        for i in range(0, len(sites), MAX_SITES_PER_ROW):
            chunk = sites[i : i + MAX_SITES_PER_ROW]
            prefix = group_name if i == 0 else f"→ {group_name}"
            rows.append([prefix, ""] + chunk)

    return rows


# ============================================================
# GOOGLE SHEETS UPDATE
# ============================================================


def update_google_sheet() -> None:
    worksheet = get_worksheet()
    studios = load_processed_studios()
    rows = build_rows(studios)

    print(f"📤 Uploading {len(rows)} rows to Google Sheets…")

    worksheet.resize(rows=len(rows) + 10, cols=COLUMN_END)

    last_col_letter = gspread.utils.rowcol_to_a1(1, COLUMN_END).split("1")[0]
    worksheet.batch_clear([f"A2:{last_col_letter}2000"])

    worksheet.update(
        values=rows,
        range_name="A2",
        value_input_option="USER_ENTERED",  # type: ignore[arg-type]
    )

    print(f"✅ Updated sheet: {SPREADSHEET_NAME} → {WORKSHEET_NAME}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    update_google_sheet()

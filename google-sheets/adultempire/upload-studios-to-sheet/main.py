"""
Google Sheets Exporter – AdultEmpire Studios
============================================

• Reads combined studio data from JSON
• Authenticates using a Google service account
• Creates worksheet if missing
• Resizes, clears, and updates sheet atomically

"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
from pathlib import Path
from typing import Any, Dict, List

# ============================================================
# THIRD-PARTY
# ============================================================

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# PATHS & CONSTANTS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

COMBINED_STUDIOS_FILE = (
    BASE_DIR
    / "scrapers"
    / "adultempire"
    / "utils"
    / "combine-all-four-studios"
    / "data"
    / "combined-studios.json"
)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

SPREADSHEET_NAME = "MY PORN"
WORKSHEET_NAME = "AdultEmpire Studios"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ============================================================
# AUTHENTICATION
# ============================================================

def get_worksheet():
    """
    Authenticate and return the target worksheet.
    Creates the worksheet if it does not exist.
    """
    credentials = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=SCOPES,
    )

    client = gspread.authorize(credentials)
    spreadsheet = client.open(SPREADSHEET_NAME)

    try:
        return spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=WORKSHEET_NAME,
            rows=1,
            cols=1,
        )

# ============================================================
# DATA LOADING
# ============================================================

def load_studios() -> List[Dict[str, Any]]:
    """
    Load combined studios JSON from disk.
    """
    with open(COMBINED_STUDIOS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# ============================================================
# UTILITIES
# ============================================================

def safe_str(value: Any) -> str:
    """
    Convert values to strings safely for Google Sheets.
    """
    return "" if value is None else str(value)

# ============================================================
# DATA TRANSFORMATION
# ============================================================

def build_sheet_rows(studios: List[Dict[str, Any]]) -> List[List[str]]:
    """
    Convert studio objects into a 2D array for Google Sheets.
    """
    header = [
        "ID",
        "Title",
        "Clips URL", "# Clips",
        "DVDs URL", "# DVDs",
        "VODs URL", "# VODs",
        "Blu-rays URL", "# Blu-rays",
    ]

    rows: List[List[str]] = [header]

    for idx, studio in enumerate(studios, start=1):
        rows.append([
            safe_str(idx),
            safe_str(studio.get("title")),
            safe_str(studio.get("clips_url")),
            safe_str(studio.get("num_clips")),
            safe_str(studio.get("dvds_url")),
            safe_str(studio.get("num_dvds")),
            safe_str(studio.get("vods_url")),
            safe_str(studio.get("num_vods")),
            safe_str(studio.get("blurays_url")),
            safe_str(studio.get("num_blurays")),
        ])

    return rows

# ============================================================
# GOOGLE SHEETS UPDATE
# ============================================================

def update_google_sheet() -> None:
    """
    Resize, clear, and update the Google Sheet with studio data.
    """
    worksheet = get_worksheet()
    studios = load_studios()
    rows = build_sheet_rows(studios)

    row_count = len(rows)
    col_count = len(rows[0])

    # Resize sheet exactly to data dimensions
    worksheet.resize(rows=row_count, cols=col_count)

    # Clear existing content
    worksheet.clear()

    # Update data
    end_col = chr(64 + col_count)  # A, B, C...
    worksheet.update(
        values=rows,
        range_name=f"A1:{end_col}{row_count}",
        value_input_option="USER_ENTERED",  # type: ignore[arg-type]
    )

    print(f"✅ Updated '{WORKSHEET_NAME}' with {row_count - 1} studios")
    print(f"🧹 Sheet resized to {row_count} rows × {col_count} columns")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    update_google_sheet()

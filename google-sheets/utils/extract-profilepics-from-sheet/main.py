"""
extract_pornstars_from_sheet.py

Reads the "Pornstars" worksheet from Google Sheet "MY PORN" and exports:

[
  {
    "name": "Performer Name",
    "image": [
        "https://image1.jpg",
        "https://image2.jpg"
    ]
  }
]

Design goals:
- Clean separation of concerns
- Safe handling of multi-link cells
- Easy to extend (new columns / new sheets)
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = (
    BASE_DIR / "google-sheets" / "utils" / "extract-profilepics-from-sheet" / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"
OUTPUT_JSON_FILE = DATA_DIR / "pornstars-profilepics-from-sheet.json"

SPREADSHEET_NAME = "MY PORN"
WORKSHEET_NAME = "Pornstars"

COL_NAME = 1  # Column B
COL_IMAGE = 30  # Column AE


# ============================================================
# GOOGLE AUTH & SERVICES
# ============================================================


def get_credentials(scopes: List[str]) -> Credentials:
    return Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=scopes,
    )


def get_drive_service():
    scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    return build("drive", "v3", credentials=get_credentials(scopes))


def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    return build("sheets", "v4", credentials=get_credentials(scopes))


def find_spreadsheet_id_by_name(name: str) -> str:
    drive = get_drive_service()
    query = (
        f"name = '{name}' " "and mimeType = 'application/vnd.google-apps.spreadsheet'"
    )
    result = drive.files().list(q=query, fields="files(id,name)").execute()

    files = result.get("files", [])
    if not files:
        raise ValueError(f"Spreadsheet '{name}' not found")

    return files[0]["id"]


# ============================================================
# SHEET DATA ACCESS
# ============================================================


def fetch_sheet_rows(spreadsheet_id: str, sheet_name: str) -> List[Dict[str, Any]]:
    service = get_sheets_service()
    response = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            ranges=[sheet_name],
            fields="sheets(data(rowData(values(userEnteredValue,hyperlink,textFormatRuns))))",
        )
        .execute()
    )

    return response["sheets"][0]["data"][0].get("rowData", [])


# ============================================================
# CELL PARSING UTILITIES
# ============================================================


def extract_text(cell: Dict[str, Any]) -> str:
    value = cell.get("userEnteredValue", {})
    return value.get("stringValue", "") if isinstance(value, dict) else ""


def extract_hyperlinks(cell: Dict[str, Any]) -> List[str]:
    """
    Extract ALL hyperlinks from a cell in order.
    Handles multi-line cells correctly.
    """
    links: List[str] = []

    # Direct hyperlink (rare but supported)
    if "hyperlink" in cell:
        links.append(cell["hyperlink"])

    # Ordered links via textFormatRuns
    for run in cell.get("textFormatRuns", []):
        link = run.get("format", {}).get("link", {}).get("uri")
        if link:
            links.append(link)

    return links


# ============================================================
# CORE PARSING LOGIC
# ============================================================


def parse_pornstars(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pornstars: List[Dict[str, Any]] = []

    for row in rows[1:]:  # Skip header
        cells = row.get("values", [])
        if not cells:
            continue

        name = extract_text(cells[COL_NAME]).strip()
        if not name:
            continue

        images = extract_hyperlinks(cells[COL_IMAGE] if len(cells) > COL_IMAGE else {})

        pornstars.append(
            {
                "name": name,
                "image": images,
            }
        )

    return pornstars


# ============================================================
# OUTPUT
# ============================================================


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"✅ JSON saved to: {path}")


# ============================================================
# ENTRY POINT
# ============================================================


def main():
    print("🔍 Locating spreadsheet...")
    sheet_id = find_spreadsheet_id_by_name(SPREADSHEET_NAME)
    print(f"📄 Spreadsheet ID: {sheet_id}")

    print("📥 Fetching worksheet data...")
    rows = fetch_sheet_rows(sheet_id, WORKSHEET_NAME)

    print("🔎 Parsing pornstars...")
    data = parse_pornstars(rows)

    print("💾 Writing JSON...")
    write_json(OUTPUT_JSON_FILE, data)

    print("🎉 Done!")


if __name__ == "__main__":
    main()

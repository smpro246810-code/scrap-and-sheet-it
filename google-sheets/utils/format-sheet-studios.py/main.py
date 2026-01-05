"""
format_networks_sheet.py

Reads a source worksheet, applies smart title-casing to all columns except A,
preserves hyperlinks, and writes the result to a target worksheet using
high-performance batch updates.
"""

from pathlib import Path
from typing import List, Dict, Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =====================================================================
# CONFIG
# =====================================================================

BASE_DIR = Path(__file__).resolve().parents[3]
GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

SPREADSHEET_ID = "1ZBydtcOO3UOOMvKOZ6b_Ze-JDnz6VRbDaqKXkjInkV0"
SOURCE_SHEET = "Networks"
TARGET_SHEET = "Copy of Networks"

SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# =====================================================================
# AUTHENTICATION
# =====================================================================

def get_sheets_service():
    """
    Return an authenticated Google Sheets API service instance.
    """
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=SHEETS_SCOPE,
    )
    return build("sheets", "v4", credentials=creds)

# =====================================================================
# DATA FETCHING
# =====================================================================

def fetch_raw_rows(sheet_name: str) -> List[Dict[str, Any]]:
    """
    Fetch full rowData (values + hyperlinks) from a worksheet.
    """
    service = get_sheets_service()
    result = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        ranges=[f"{sheet_name}!A2:ZZ"],
        fields="sheets(data(rowData(values(userEnteredValue,textFormatRuns,hyperlink))))",
    ).execute()

    sheets = result.get("sheets", [])
    if not sheets:
        return []

    data = sheets[0].get("data", [])
    return data[0].get("rowData", []) if data else []

# =====================================================================
# TEXT FORMATTING
# =====================================================================

def smart_title_case(text: str) -> str:
    """
    Smart title-casing rules:
    - Keeps ALL CAPS words intact (XXX)
    - Keeps mixed-case words intact (TeenFlicks)
    - Preserves digits and alphanumeric words
    - Title-cases plain words
    """
    if not isinstance(text, str) or not text:
        return ""

    text = text.replace("?", "")
    words = []

    for word in text.split():
        if any(ch.isdigit() for ch in word):
            words.append(word)
        elif word.isupper():
            words.append(word)
        elif not (word.islower() or word.isupper()):
            words.append(word)
        else:
            words.append(word.title())

    return " ".join(words)

# =====================================================================
# CELL TRANSFORMATION
# =====================================================================

def format_cell(cell: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a single cell:
    - Apply smart title-case
    - Preserve hyperlink if present
    """
    raw_value = cell.get("userEnteredValue", {}).get("stringValue", "") or ""
    hyperlink = cell.get("hyperlink")

    formatted_text = smart_title_case(raw_value)

    if hyperlink:
        return {
            "userEnteredValue": {"stringValue": formatted_text},
            "textFormatRuns": [
                {"startIndex": 0, "format": {"link": {"uri": hyperlink}}}
            ],
        }

    return {"userEnteredValue": {"stringValue": formatted_text}}

# =====================================================================
# SHEET MANAGEMENT
# =====================================================================

def ensure_target_sheet(service) -> None:
    """
    Ensure the target worksheet exists.
    """
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()

    existing = {s["properties"]["title"] for s in meta["sheets"]}

    if TARGET_SHEET not in existing:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "requests": [
                    {"addSheet": {"properties": {"title": TARGET_SHEET}}}
                ]
            },
        ).execute()
        print(f"✅ Created worksheet: {TARGET_SHEET}")
    else:
        print(f"ℹ️ Worksheet '{TARGET_SHEET}' already exists — overwriting.")

def get_sheet_id(service, title: str) -> int:
    """
    Resolve numeric sheetId from sheet title.
    """
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()

    for sheet in meta["sheets"]:
        if sheet["properties"]["title"] == title:
            return sheet["properties"]["sheetId"]

    raise ValueError(f"❌ Sheet '{title}' not found")

def resize_sheet(service, sheet_id: int, rows: int, cols: int) -> None:
    """
    Resize grid before batch writing.
    """
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": rows,
                                "columnCount": cols,
                            },
                        },
                        "fields": "gridProperties(rowCount,columnCount)",
                    }
                }
            ]
        },
    ).execute()

# =====================================================================
# BATCH WRITE
# =====================================================================

def write_formatted_sheet(rows: List[List[Dict[str, Any]]]) -> None:
    """
    Write formatted data using updateCells (fastest method).
    """
    service = get_sheets_service()
    sheet_id = get_sheet_id(service, TARGET_SHEET)

    row_count = len(rows) + 2
    col_count = max((len(r) for r in rows), default=1)

    resize_sheet(service, sheet_id, row_count, col_count)

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 0,
                        },
                        "rows": [{"values": row} for row in rows],
                        "fields": "userEnteredValue,textFormatRuns",
                    }
                }
            ]
        },
    ).execute()

    print(f"✅ Updated '{TARGET_SHEET}' with {len(rows)} formatted rows.")

# =====================================================================
# MAIN WORKFLOW
# =====================================================================

def main() -> None:
    print("📥 Fetching source sheet...")
    raw_rows = fetch_raw_rows(SOURCE_SHEET)

    if not raw_rows:
        print("❌ No data found.")
        return

    service = get_sheets_service()
    ensure_target_sheet(service)

    formatted_rows: List[List[Dict[str, Any]]] = []

    for row in raw_rows:
        cells = row.get("values", [])

        # Column A preserved exactly
        col_a = cells[0] if cells else {"userEnteredValue": {"stringValue": ""}}

        # Columns B+ formatted
        formatted_rest = [format_cell(c) for c in cells[1:]]

        formatted_rows.append([col_a] + formatted_rest)

    write_formatted_sheet(formatted_rows)
    print("🎉 Done!")

# =====================================================================
# ENTRY POINT
# =====================================================================

if __name__ == "__main__":
    main()

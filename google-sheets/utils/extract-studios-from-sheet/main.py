"""
extract_networks_from_sheet.py

Purpose:
--------
Read the "Networks" worksheet from a Google Sheet and export a clean JSON
representation of networks and their associated sites.

Output format:
--------------
[
  {
    "title": "Network Name",
    "url": "https://...",
    "sites": [
      {"title": "Site Name", "url": "https://..."},
      ...
    ]
  }
]
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =====================================================================
# CONFIG
# =====================================================================

BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "extract-studios-from-sheet"
    / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"
OUTPUT_JSON_FILE = DATA_DIR / "studios-from-sheet.json"

SPREADSHEET_NAME = "MY PORN"
WORKSHEET_NAME = "Networks"

# Column indices (0-based)
COL_NETWORK = 1       # Column B
COL_SITES_START = 3  # Column D onward

# =====================================================================
# AUTH & SERVICES
# =====================================================================

def load_credentials(scopes: List[str]) -> Credentials:
    """Load Google service-account credentials."""
    return Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=scopes,
    )


def drive_service():
    """Return Google Drive API service."""
    scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    creds = load_credentials(scopes)
    return build("drive", "v3", credentials=creds)


def sheets_service(readonly: bool = True):
    """Return Google Sheets API service."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly"
        if readonly else
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    creds = load_credentials(scopes)
    return build("sheets", "v4", credentials=creds)


# =====================================================================
# SPREADSHEET RESOLUTION
# =====================================================================

def find_spreadsheet_id(spreadsheet_name: str) -> str:
    """
    Resolve spreadsheet ID from its title using Drive API.
    """
    service = drive_service()

    query = (
        f"name = '{spreadsheet_name}' "
        "and mimeType = 'application/vnd.google-apps.spreadsheet'"
    )

    result = service.files().list(
        q=query,
        fields="files(id,name)"
    ).execute()

    files = result.get("files", [])
    if not files:
        raise RuntimeError(f"Spreadsheet '{spreadsheet_name}' not found.")

    return files[0]["id"]


# =====================================================================
# SHEET DATA FETCH
# =====================================================================

def fetch_row_data(spreadsheet_id: str, worksheet: str) -> List[Dict[str, Any]]:
    """
    Fetch full rowData (including hyperlinks & formatting) from a worksheet.
    """
    service = sheets_service()

    response = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[worksheet],
        fields="sheets(data(rowData(values(userEnteredValue,textFormatRuns,hyperlink))))"
    ).execute()

    return (
        response
        .get("sheets", [{}])[0]
        .get("data", [{}])[0]
        .get("rowData", [])
    )


# =====================================================================
# CELL EXTRACTION HELPERS
# =====================================================================

def cell_text(cell: Dict[str, Any]) -> str:
    """Extract plain text from a cell."""
    uev = cell.get("userEnteredValue", {})
    return uev.get("stringValue", "") if isinstance(uev, dict) else ""


def cell_hyperlink(cell: Dict[str, Any]) -> Optional[str]:
    """Extract hyperlink from cell if present."""
    if "hyperlink" in cell:
        return cell["hyperlink"]

    for run in cell.get("textFormatRuns", []):
        link = run.get("format", {}).get("link")
        if link:
            return link.get("uri")

    return None


# =====================================================================
# PARSER
# =====================================================================

def parse_networks(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert rowData into structured network objects.
    """
    networks: List[Dict[str, Any]] = []

    # Skip header row
    for row in rows[1:]:
        values = row.get("values", [])
        if not values:
            continue

        network_cell = values[COL_NETWORK] if len(values) > COL_NETWORK else {}
        network_title = cell_text(network_cell).strip()

        if not network_title:
            continue

        network_url = cell_hyperlink(network_cell)

        sites = []
        for cell in values[COL_SITES_START:]:
            title = cell_text(cell).strip()
            if title:
                sites.append({
                    "title": title,
                    "url": cell_hyperlink(cell),
                })

        networks.append({
            "title": network_title,
            "url": network_url,
            "sites": sites,
        })

    return networks


# =====================================================================
# OUTPUT
# =====================================================================

def write_json(path: Path, data: Any) -> None:
    """Write formatted JSON to disk."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ JSON written to {path}")


# =====================================================================
# MAIN
# =====================================================================

def main() -> None:
    print("🔍 Resolving spreadsheet ID...")
    spreadsheet_id = find_spreadsheet_id(SPREADSHEET_NAME)
    print(f"📄 Spreadsheet ID: {spreadsheet_id}")

    print("📥 Fetching worksheet data...")
    rows = fetch_row_data(spreadsheet_id, WORKSHEET_NAME)

    print("🔎 Parsing networks...")
    networks = parse_networks(rows)

    print("💾 Writing output...")
    write_json(OUTPUT_JSON_FILE, networks)

    print("🎉 Done!")


if __name__ == "__main__":
    main()

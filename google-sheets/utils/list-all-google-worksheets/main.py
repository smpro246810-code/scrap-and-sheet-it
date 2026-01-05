"""
List Google Sheets Worksheets
=============================

• Locate spreadsheet by name using Drive API
• Fetch all worksheet metadata via Sheets API
• Optionally filter worksheets by FEMALE pornstar names
• Save results as JSON using an interactive menu

"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
from pathlib import Path
from typing import List, Dict, Any, Set

# ============================================================
# THIRD-PARTY
# ============================================================

import inquirer
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ============================================================
# PATHS & CONSTANTS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

FEMALE_AND_TRANS_PORNSTARS_FILE = (
    BASE_DIR
    / "scrapers"
    / "data18"
    / "utils"
    / "extract-female-and-trans-pornstars"
    / "data"
    / "female-and-trans-pornstars.json"
)

SPREADSHEET_NAME = "MY PORN"

OUTPUT_DIR = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "list-all-google-worksheets"
    / "data"
)

OUTPUT_FEMALE_AND_TRANS_FILE = OUTPUT_DIR / "female-and-trans-pornstar-worksheets.json"
OUTPUT_EXCEPT_FEMALE_AND_TRANS_FILE = (
    OUTPUT_DIR / "except-female-and-trans-pornstar-worksheets.json"
)
OUTPUT_ALL_FILE = OUTPUT_DIR / "all-available-worksheets.json"

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# ============================================================
# AUTH HELPERS
# ============================================================

def get_credentials(scopes: List[str]) -> Credentials:
    return Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=scopes,
    )


def get_drive_api():
    creds = get_credentials(DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)


def get_sheets_api():
    creds = get_credentials(SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

# ============================================================
# GOOGLE SHEETS DISCOVERY
# ============================================================

def find_spreadsheet_id(spreadsheet_name: str) -> str:
    """
    Locate spreadsheet ID by name using Drive API.
    """
    drive = get_drive_api()
    query = (
        f"name = '{spreadsheet_name}' "
        "and mimeType = 'application/vnd.google-apps.spreadsheet'"
    )

    result = drive.files().list(
        q=query,
        fields="files(id,name)",
    ).execute()

    files = result.get("files", [])
    if not files:
        raise RuntimeError(f"Spreadsheet '{spreadsheet_name}' not found.")

    return files[0]["id"]

# ============================================================
# WORKSHEET LOADING
# ============================================================

def load_all_worksheets(spreadsheet_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all available worksheets from a spreadsheet.

    Preserves UI / creation order using `index`.
    """
    sheets_api = get_sheets_api()
    info = sheets_api.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=False,
    ).execute()

    worksheets: List[Dict[str, Any]] = []

    for sheet in info.get("sheets", []):
        props = sheet.get("properties", {})
        worksheets.append({
            "title": props.get("title"),
            "hidden": props.get("hidden", False),
            "sheet_id": props.get("sheetId"),
            "index": props.get("index", 0),
        })

    worksheets.sort(key=lambda x: x["index"])
    return worksheets

# ============================================================
# FEMALE_AND_TRANS PORNSTAR LOADING
# ============================================================

def normalize(text: str) -> str:
    return " ".join(text.strip().lower().split())


def load_female_and_trans_pornstar_names() -> Set[str]:
    """
    Load and normalize female-and-trans pornstar names
    from Data18 female-and-trans-pornstars.json.
    """
    if not FEMALE_AND_TRANS_PORNSTARS_FILE.exists():
        raise RuntimeError(
            f"Pornstar file missing: {FEMALE_AND_TRANS_PORNSTARS_FILE}"
        )

    raw = json.loads(
        FEMALE_AND_TRANS_PORNSTARS_FILE.read_text(encoding="utf-8")
    )

    data = raw.get("data", [])
    if not isinstance(data, list):
        raise RuntimeError(
            "Invalid female-and-trans-pornstars.json format: 'data' must be a list"
        )

    return {
        normalize(entry.get("name", ""))
        for entry in data
        if entry.get("name")
    }

# ============================================================
# FILTERING
# ============================================================

def filter_female_and_trans_pornstar_sheets(
    worksheets: List[Dict[str, Any]],
    pornstar_names: Set[str],
) -> List[Dict[str, Any]]:
    """
    Keep only worksheets whose titles match pornstar names.
    """
    return [
        ws
        for ws in worksheets
        if normalize(ws.get("title", "")) in pornstar_names
    ]

def filter_except_female_and_trans_pornstar_sheets(
    worksheets: List[Dict[str, Any]],
    pornstar_names: Set[str],
) -> List[Dict[str, Any]]:
    """
    Keep worksheets whose titles do NOT match female pornstar names.
    """
    return [
        ws
        for ws in worksheets
        if normalize(ws.get("title", "")) not in pornstar_names
    ]

# ============================================================
# JSON OUTPUT
# ============================================================

def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"💾 Saved: {path}")

# ============================================================
# MAIN FLOW
# ============================================================

def main() -> None:
    print("🔍 Loading female-and-trans pornstar list")
    pornstar_names = load_female_and_trans_pornstar_names()
    print(f"✔ Loaded {len(pornstar_names)} female-and-trans pornstar names.")

    print("\n🔍 Locating spreadsheet...")
    spreadsheet_id = find_spreadsheet_id(SPREADSHEET_NAME)
    print(f"✔ Spreadsheet ID: {spreadsheet_id}")

    print("\n📥 Fetching ALL available worksheets...")
    all_worksheets = load_all_worksheets(spreadsheet_id)
    print(f"✔ Total worksheets found: {len(all_worksheets)}")

    # -------- Interactive Selection --------
    answer = inquirer.prompt([
        inquirer.List(
            "mode",
            message="Select which worksheets to save",
            choices=[
                "Only FEMALE-AND-TRANS pornstar worksheets",
                "Except FEMALE-AND-TRANS pornstar worksheets",
                "ALL available worksheets",
            ],
        )
    ])

    if not answer:
        print("❌ No option selected.")
        return

    mode = answer["mode"]

    # -------- Processing --------
    if mode == "Only FEMALE-AND-TRANS pornstar worksheets":
        print("\n🔎 Filtering female-and-trans pornstar worksheets...")
        filtered = filter_female_and_trans_pornstar_sheets(
            all_worksheets,
            pornstar_names,
        )
        print(f"✔ Found {len(filtered)} female-and-trans pornstar worksheets.")
        save_json(OUTPUT_FEMALE_AND_TRANS_FILE, filtered)

    elif mode == "Except FEMALE-AND-TRANS pornstar worksheets":
        print("\n🔎 Filtering except female-and-trans pornstar worksheets...")
        filtered = filter_except_female_and_trans_pornstar_sheets(
            all_worksheets,
            pornstar_names,
        )
        print(f"✔ Found {len(filtered)} except female-and-trans pornstar worksheets.")
        save_json(OUTPUT_EXCEPT_FEMALE_AND_TRANS_FILE, filtered)

    else:
        print("\n🔎 Keeping ALL available worksheets...")
        save_json(OUTPUT_ALL_FILE, all_worksheets)

    print("\n🎉 DONE.")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

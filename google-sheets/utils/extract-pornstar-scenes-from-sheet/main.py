#!/usr/bin/env python3
"""
export_pornstar_scenes_to_json.py
"""

import json
import sys
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "extract-pornstar-scenes-from-sheet"
    / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"
SPREADSHEET_NAME = "MY PORN"

WORKSHEET_LIST_FILE = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "list-all-google-worksheets"
    / "data"
    / "female-and-trans-pornstar-worksheets.json"
)

# ============================================================
# COLUMN INDICES (0-BASED)
# ============================================================

COL_ID = 0
COL_PORNSTAR = 1
COL_SCENE_ID = 2
COL_DATE = 3
COL_MALE_PARTNERS = 4
COL_FEMALE_AND_TRANS_PARTNERS = 5
COL_NETWORK_OR_STUDIO = 6
COL_SITE_OR_WEBSERIE = 7
COL_TITLE = 8
COL_BANNER = 9
COL_IS_VR_VIDEO = 10
COL_IS_TELE_SAVE = 11
COL_TELE_LINK = 12
COL_QUALITY = 13
COL_FILE_SIZE = 14
COL_DURATION = 15
COL_THUMBNAILS = 16
COL_SCREENCAPS = 17
COL_PICS_SETS = 18
COL_VIDEO_LINKS = 19
COL_ORIGINAL_URL = 20
COL_DATA18_OR_IAFD_URL = 21
COL_DATA18_TRAILER_URL = 22
COL_TELELABEL = 23

# ============================================================
# AUTH
# ============================================================

def get_credentials(scopes: List[str]) -> Credentials:
    return Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)

def get_sheets_api():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    return build("sheets", "v4", credentials=get_credentials(scopes))

def find_spreadsheet_id_by_name(name: str) -> str:
    drive = build(
        "drive",
        "v3",
        credentials=get_credentials(
            ["https://www.googleapis.com/auth/drive.metadata.readonly"]
        ),
    )
    result = drive.files().list(
        q=f"name='{name}' and mimeType='application/vnd.google-apps.spreadsheet'",
        fields="files(id)",
    ).execute()

    files = result.get("files", [])
    if not files:
        raise RuntimeError(f"Spreadsheet not found: {name}")
    return files[0]["id"]

# ============================================================
# GLOBAL RICH ROW HOLDER (COL-BASED)
# ============================================================

CURRENT_RICH_ROW: Dict[int, Dict[str, Any]] = {}

# ============================================================
# RICH CELL HELPERS
# ============================================================

def is_magenta_text(col_index: int) -> bool:
    cell = CURRENT_RICH_ROW.get(col_index)

    if not cell:
        return False

    text_fmt = (
        cell
        .get("effectiveFormat", {})
        .get("textFormat", {})
    )

    rgb = (
        text_fmt
        .get("foregroundColorStyle", {})
        .get("rgbColor")
    )

    if not rgb:
        return False

    r = rgb.get("red", 0)
    g = rgb.get("green", 0)
    b = rgb.get("blue", 0)

    # ✅ STRICT MAGENTA CHECK
    is_magenta = r == 1 and g == 0 and b == 1

    return is_magenta


def extract_hyperlink(col_index: int) -> Optional[str]:
    cell = CURRENT_RICH_ROW.get(col_index)
    if not cell:
        return None

    if "hyperlink" in cell:
        return cell["hyperlink"]

    for run in cell.get("textFormatRuns", []):
        link = run.get("format", {}).get("link")
        if link:
            return link.get("uri")

    formula = cell.get("userEnteredValue", {}).get("formulaValue")
    if isinstance(formula, str):
        m = re.search(r'HYPERLINK\("([^"]+)"', formula, re.I)
        if m:
            return m.group(1)

    return None

# ============================================================
# VALUE HELPERS
# ============================================================

def yes_no_to_bool(v: str) -> bool:
    return str(v).strip().lower() == "yes"

def cell_to_list(v: str) -> List[str]:
    return [x.strip() for x in str(v).split("\n") if x.strip()]

def cell_to_list_split_comma(v: str) -> List[str]:
    return [x.strip() for x in str(v).replace(";", ",").split(",") if x.strip()]

def parse_excel_date(value):
    try:
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=float(value))).strftime("%B %d, %Y")
    except:
        return value

# ============================================================
# PERFORMER PARSER
# ============================================================

_SCENE_COUNT_RE = re.compile(r"^(.*?)\s*\{(\d+)\}$")

def parse_performers_with_counts(cell_value: str) -> List[Dict[str, int]]:
    performers = []
    for line in cell_to_list(cell_value):
        m = _SCENE_COUNT_RE.match(line)
        if m:
            performers.append({"name": m.group(1), "scenes_count": int(m.group(2))})
        else:
            performers.append({"name": line, "scenes_count": 0})
    return performers

# ============================================================
# SCENE PARSER
# ============================================================

def parse_scene_row(row: List[str]) -> Dict[str, Any]:
    female_and_trans = parse_performers_with_counts(row[COL_FEMALE_AND_TRANS_PARTNERS])

    female, trans = [], []
    for p in female_and_trans:
        if "(trans)" in p["name"].lower():
            trans.append({
                "name": p["name"].replace("(trans)", "").strip(),
                "scenes_count": p["scenes_count"],
            })
        else:
            female.append(p)

    return {
        "id": row[COL_ID],
        "is_favorite": is_magenta_text(COL_PORNSTAR),
        "scene_id": row[COL_SCENE_ID],
        "release_date": parse_excel_date(row[COL_DATE]),

        "partners": {
            "male": parse_performers_with_counts(row[COL_MALE_PARTNERS]),
            "female": female,
            "trans": trans,
        },

        "network": {
            "name": row[COL_NETWORK_OR_STUDIO],
            "pair_url": extract_hyperlink(COL_NETWORK_OR_STUDIO),
        },
        "site": {
            "name": row[COL_SITE_OR_WEBSERIE],
            "pair_url": extract_hyperlink(COL_SITE_OR_WEBSERIE),
        },

        "title": row[COL_TITLE],
        "banner": row[COL_BANNER],
        "is_vr_video": yes_no_to_bool(row[COL_IS_VR_VIDEO]),

        "quality": cell_to_list_split_comma(row[COL_QUALITY]),
        "file_size": row[COL_FILE_SIZE],
        "duration": row[COL_DURATION],

        "assets": {
            "thumbnails": cell_to_list(row[COL_THUMBNAILS]),
            "screencaps": cell_to_list(row[COL_SCREENCAPS]),
            "pics_set": cell_to_list(row[COL_PICS_SETS]),
            "video_links": cell_to_list(row[COL_VIDEO_LINKS]),
        },

        "urls": {
            "original": cell_to_list(row[COL_ORIGINAL_URL]),
            "data18/iafd": cell_to_list(row[COL_DATA18_OR_IAFD_URL]),
            "data18_trailer": cell_to_list(row[COL_DATA18_TRAILER_URL]),
        },

        "telelabel": row[COL_TELELABEL],
    }

# ============================================================
# RICH CELL FETCHER (COL-BASED)
# ============================================================

def fetch_rich_cells(spreadsheet_id: str, tab_name: str) -> List[Dict[int, Dict[str, Any]]]:
    sheets = get_sheets_api()
    resp = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[
            f"{tab_name}!B2:B",
            f"{tab_name}!G2:G",
            f"{tab_name}!H2:H",
        ],
        includeGridData=True,
        fields="sheets.data.startColumn,sheets.data.rowData.values",
    ).execute()

    rows: Dict[int, Dict[int, Dict[str, Any]]] = {}

    for sheet in resp["sheets"]:
        for data in sheet["data"]:
            start_col = data.get("startColumn", 0)
            for r, row in enumerate(data.get("rowData", [])):
                rows.setdefault(r, {})
                for c, cell in enumerate(row.get("values", [])):
                    col = start_col + c
                    if col in (COL_PORNSTAR, COL_NETWORK_OR_STUDIO, COL_SITE_OR_WEBSERIE):
                        rows[r][col] = cell

    return [rows[i] for i in sorted(rows)]

# ============================================================
# EXPORT
# ============================================================

def export_pornstar_tab(spreadsheet_id: str, tab_name: str):
    sheets = get_sheets_api()

    # 1️⃣ Normal values
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A2:Z",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])

    # 2️⃣ Banner formulas only (column J)
    banner_formulas = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!J2:J",
        valueRenderOption="FORMULA",
    ).execute().get("values", [])

    # 3️⃣ Rich cells (B, G, H)
    rich_rows = fetch_rich_cells(spreadsheet_id, tab_name)

    scenes = []

    for i, row in enumerate(rows):
        # Pad row to full width
        row = row + [""] * (COL_TELELABEL + 1 - len(row))

        # Skip completely empty rows
        if not any(str(v).strip() for v in row):
            continue

        # Inject banner formula
        if i < len(banner_formulas) and banner_formulas[i]:
            row[COL_BANNER] = banner_formulas[i][0]

        # Expose rich cells to parser
        global CURRENT_RICH_ROW
        CURRENT_RICH_ROW = rich_rows[i] if i < len(rich_rows) else {}

        scenes.append(parse_scene_row(row))

    outfile = DATA_DIR / f"{tab_name.lower().replace(' ', '-')}.json"
    outfile.write_text(
        json.dumps(scenes, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"✅ Exported {len(scenes)} scenes → {outfile}")

# ============================================================
# MAIN
# ============================================================

def main():
    spreadsheet_id = find_spreadsheet_id_by_name(SPREADSHEET_NAME)
    tabs = json.loads(WORKSHEET_LIST_FILE.read_text(encoding="utf-8"))

    print("\nSelect worksheet:\n")
    for i, t in enumerate(tabs, 1):
        print(f"{i:3d}. {t['title']}")

    idx = int(input("\nChoice: ")) - 1
    export_pornstar_tab(spreadsheet_id, tabs[idx]["title"])

if __name__ == "__main__":
    main()

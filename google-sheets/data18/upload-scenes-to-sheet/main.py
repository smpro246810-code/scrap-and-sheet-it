import json
from pathlib import Path
from typing import List

import inquirer
from gspread.cell import Cell

from helpers import (
    extract_pornstar_from_filename,
    normalize_name,
    safe_load_json,
    get_worksheet,
    parse_args,
    norm_scene_id,
)
from scene_flatten import flatten_scene_to_row
from sheet_state import build_sceneid_index, find_empty_template_rows
from sheet_writer import update_existing_row, write_new_row_from_template


# ============================================================
# Sheet layout configuration
# ============================================================

MAX_COLS = 23

# Columns that are allowed to be updated when a scene already exists
# (0-based indexing, aligned with flatten_scene_to_row output)
UPDATEABLE_COLUMNS = {
    1,                      # B: Pornstar
    *range(2, 9),           # C–I: Scene ID → Title
    11,                     # L: TeleLink
    *range(13, 22),         # N–V: Quality → Data18 / IAFD URL
}


# ============================================================
# File paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

SCENES_DIR = (
    BASE_DIR
    / "scrapers"
    / "data18"
    / "main-scraper"
    / "data"
)
SCENES_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

MALE_FILE = (
    BASE_DIR
    / "scrapers"
    / "utils"
    / "merge-male-pornstars"
    / "data"
    / "merged-male-pornstars.json"
)

TRANS_FILE = (
    BASE_DIR
    / "scrapers"
    / "adultempire"
    / "pornstars-scraper"
    / "data"
    / "ts-pornstars.json"
)

NETWORKS_FILE = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "extract-studios-from-sheet"
    / "data"
    / "studios-from-sheet.json"
)


# ============================================================
# Load reference data
# ============================================================

male_performers = {
    normalize_name(p["name"]) for p in (safe_load_json(MALE_FILE) or [])
}
trans_performers = {
    normalize_name(p["name"]) for p in (safe_load_json(TRANS_FILE) or [])
}

networks = safe_load_json(NETWORKS_FILE) or []
site_to_network = {
    normalize_name(s["title"]): n["title"]
    for n in networks
    for s in n.get("sites", [])
}


# ============================================================
# Utility helpers
# ============================================================

def select_json_file() -> Path:
    files = sorted(SCENES_DIR.glob("*.json"))
    answer = inquirer.prompt([
        inquirer.List(
            "file",
            message="Select performer JSON:",
            choices=[f.name for f in files],
        )
    ])
    return SCENES_DIR / answer["file"]


def load_scenes(path: Path) -> List[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_rows(sheet_values: List[List[str]]) -> List[List[str]]:
    rows = sheet_values[1:]
    return [r + [""] * (MAX_COLS - len(r)) for r in rows]


# ============================================================
# Main update logic
# ============================================================

def update_google_sheet_from_file(hyperlinks_enabled: bool):
    json_file = select_json_file()
    pornstar = extract_pornstar_from_filename(json_file)

    ws = get_worksheet(pornstar)
    scenes = load_scenes(json_file)

    # --------------------------------------------------------
    # Flatten scenes → (row, performer_links)
    # --------------------------------------------------------

    rows: List[list] = []
    performer_link_maps: List[dict] = []

    for scene in scenes:
        row, performer_links = flatten_scene_to_row(
            scene,
            pornstar,
            male_performers,
            trans_performers,
            site_to_network,
            hyperlinks_enabled,
            lambda x: x,
        )
        rows.append(row + [""] * (MAX_COLS - len(row)))
        performer_link_maps.append(performer_links)

    # --------------------------------------------------------
    # Read sheet state
    # --------------------------------------------------------

    sheet_values = ws.get_all_values(value_render_option="FORMULA")
    existing_rows = normalize_rows(sheet_values)

    scene_index = build_sceneid_index(existing_rows)
    free_rows = find_empty_template_rows(existing_rows)

    # --------------------------------------------------------
    # Prepare batch updates
    # --------------------------------------------------------

    batch_cells: List[Cell] = []
    rich_text_requests: list = []
    free_idx = 0
    sheet_id = ws._properties["sheetId"]

    # --------------------------------------------------------
    # Apply updates
    # --------------------------------------------------------

    for i, row in enumerate(rows):
        sid = norm_scene_id(row[2])

        # Existing scene → update
        if sid in scene_index:
            rnum = scene_index[sid]
            update_existing_row(
                batch_cells,
                rich_text_requests,
                sheet_id,
                rnum,
                row,
                existing_rows[rnum - 2],
                UPDATEABLE_COLUMNS,
                performer_link_maps[i],
            )

        # New scene → template row
        elif free_idx < len(free_rows):
            write_new_row_from_template(
                batch_cells,
                free_rows[free_idx],
                row,
                22,  # TeleLabel column index
            )
            free_idx += 1

    # --------------------------------------------------------
    # Execute batch writes
    # --------------------------------------------------------

    if batch_cells:
        ws.update_cells(batch_cells, value_input_option="USER_ENTERED")

    if rich_text_requests:
        ws.spreadsheet.batch_update({
            "requests": rich_text_requests
        })


# ============================================================
# Script entry point
# ============================================================

if __name__ == "__main__":
    args = parse_args()
    update_google_sheet_from_file(args.hyperlinks == "on")

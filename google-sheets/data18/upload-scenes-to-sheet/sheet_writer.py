from typing import List, Dict
from gspread.cell import Cell

from helpers import normalize_name


# ============================================================
# Column index constants
# ============================================================

DATA18_URL_INDEX = 21        # Column V
DURATION_INDEX = 15          # Column P
PERFORMER_COLUMNS = {4, 5}   # Columns E & F


# ============================================================
# Rich text → RAW API request builder
# ============================================================

def build_performer_rich_text_request(
    sheet_id: int,
    row: int,
    col: int,
    cell_text: str,
    performer_url_map: Dict[str, str],
):
    """
    Build a raw Google Sheets API request for rich-text hyperlinks
    (multiple performers in one cell).
    """
    text_runs = []
    index = 0
    lines = cell_text.split("\n")

    for i, line in enumerate(lines):
        name = line.split("{")[0].strip()
        norm = normalize_name(name)
        url = performer_url_map.get(norm)

        run = {
            "startIndex": index,
        }

        if url:
            run["format"] = {
                "link": {"uri": url}
            }

        text_runs.append(run)
        index += len(line)

        if i < len(lines) - 1:
            index += 1  # newline

    return {
        "updateCells": {
            "rows": [{
                "values": [{
                    "userEnteredValue": {
                        "stringValue": cell_text
                    },
                    "textFormatRuns": text_runs,
                }]
            }],
            "fields": "userEnteredValue,textFormatRuns",
            "start": {
                "sheetId": sheet_id,
                "rowIndex": row - 1,
                "columnIndex": col - 1,
            },
        }
    }


# ============================================================
# URL merge helper
# ============================================================

def merge_urls(old: str, new: str) -> str:
    old_urls = {u.strip() for u in str(old).split("\n") if u.strip()}
    new_urls = {u.strip() for u in str(new).split("\n") if u.strip()}
    return "\n".join(sorted(old_urls | new_urls))


# ============================================================
# Existing row update
# ============================================================

def update_existing_row(
    batch_cells: List[Cell],
    rich_text_requests: list,
    sheet_id: int,
    target_rownum: int,
    new_row: list,
    old_row: list,
    updateable_columns: set,
    performer_url_map: Dict[str, str],
):
    """
    Compare an existing sheet row with a newly generated row
    and enqueue only the changed cells for batch update.
    """
    for c in updateable_columns:
        new_val = new_row[c]
        old_val = old_row[c]

        # 🎭 Performer columns → rich text API
        if c in PERFORMER_COLUMNS and isinstance(new_val, str) and new_val.strip():
            rich_text_requests.append(
                build_performer_rich_text_request(
                    sheet_id,
                    target_rownum,
                    c + 1,
                    new_val,
                    performer_url_map,
                )
            )
            continue

        # 🔒 Duration protection
        if c == DURATION_INDEX:
            if str(old_val).strip():
                continue
            if str(new_val).strip():
                batch_cells.append(
                    Cell(row=target_rownum, col=c + 1, value=new_val)
                )
            continue

        # 🔥 URL merge
        if c == DATA18_URL_INDEX:
            merged = merge_urls(old_val, new_val)
            if merged != (old_val or ""):
                batch_cells.append(
                    Cell(row=target_rownum, col=c + 1, value=merged)
                )
            continue

        # Default overwrite
        if (new_val or "") != (old_val or ""):
            batch_cells.append(
                Cell(row=target_rownum, col=c + 1, value=new_val)
            )


# ============================================================
# New row from template
# ============================================================

def write_new_row_from_template(
    batch_cells: List[Cell],
    sheet_rownum: int,
    row_with_offset: list,
    telelabel_index: int,
):
    for c, value in enumerate(row_with_offset):
        if c == telelabel_index:
            continue
        batch_cells.append(
            Cell(row=sheet_rownum, col=c + 1, value=value)
        )

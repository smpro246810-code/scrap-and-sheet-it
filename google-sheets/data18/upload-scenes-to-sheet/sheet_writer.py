from typing import List
from gspread.cell import Cell

# ============================================================
# Column index constants
# ============================================================

# Column V (0-based index 21)
# Used for Data18 URL + manually added URLs (e.g., IAFD)
DATA18_URL_INDEX = 21


def merge_urls(old: str, new: str) -> str:
    """
    Merge URLs from the existing sheet cell and the incoming JSON value.

    Purpose:
    - Preserve URLs manually added by the user in Google Sheets
      (e.g., IAFD links)
    - Add new URLs coming from JSON without overwriting existing ones
    - Remove duplicates automatically

    Behavior:
    - URLs are split by newline
    - Empty values are ignored
    - Final result is sorted and joined by newline

    Args:
        old: Existing cell value from Google Sheet
        new: New value coming from JSON

    Returns:
        str: Merged, de-duplicated, newline-separated URLs
    """
    old_urls = {u.strip() for u in str(old).split("\n") if u.strip()}
    new_urls = {u.strip() for u in str(new).split("\n") if u.strip()}

    merged = list(old_urls | new_urls)
    return "\n".join(sorted(merged))


def update_existing_row(
    batch_cells: List[Cell],
    target_rownum: int,
    new_row: list,
    old_row: list,
    updateable_columns: set,
):
    """
    Compare an existing sheet row with a newly generated row
    and enqueue only the changed cells for batch update.

    Special handling:
    - Column V (Data18 / IAFD URL) is MERGED, not overwritten

    This function does NOT write directly to the sheet.
    It only appends Cell objects to `batch_cells`.

    Args:
        batch_cells: Shared list collecting Cell updates
        target_rownum: Sheet row number being updated (1-based)
        new_row: Newly generated row data
        old_row: Existing row data from the sheet
        updateable_columns: Set of column indexes allowed to be updated
    """
    for c in updateable_columns:
        new_val = new_row[c]
        old_val = old_row[c]

        # 🔥 Special case: Data18 / IAFD URL column
        # We MERGE instead of overwrite to preserve manual edits
        if c == DATA18_URL_INDEX:
            merged = merge_urls(old_val, new_val)
            if merged != (old_val or ""):
                batch_cells.append(
                    Cell(row=target_rownum, col=c + 1, value=merged)
                )
            continue

        # Default behavior: overwrite only if value actually changed
        if (new_val or "") != (old_val or ""):
            batch_cells.append(
                Cell(row=target_rownum, col=c + 1, value=new_val)
            )


def write_new_row_from_template(
    batch_cells: List[Cell],
    sheet_rownum: int,
    row_with_offset: list,
    telelabel_index: int,
):
    """
    Write a brand-new scene row into a pre-existing template row.

    Key rule:
    - TeleLabel column MUST NOT be overwritten
      (it is maintained manually or by template logic)

    This function enqueues Cell updates only.
    Actual writing is done later via batch update.

    Args:
        batch_cells: Shared list collecting Cell updates
        sheet_rownum: Target sheet row number (1-based)
        row_with_offset: Fully prepared row aligned to sheet columns
        telelabel_index: Column index of TeleLabel (to be skipped)
    """
    for c, value in enumerate(row_with_offset):
        if c == telelabel_index:
            continue  # Preserve TeleLabel
        batch_cells.append(
            Cell(row=sheet_rownum, col=c + 1, value=value)
        )

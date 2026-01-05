from typing import Dict, List
from helpers import norm_scene_id

# ============================================================
# Sheet layout constants
# ============================================================
MAX_COLS = 24

ID_INDEX = 0                 # Column A → ID (template)
THUMBNAIL_IMAGE_INDEX = 9    # Column J → Banner (template)
TELESAVE_INDEX = 11          # Column K → TeleSave? (template)
QUALITY_INDEX = 13           # Column M → Quality? (template)
TELELABEL_INDEX = 23         # Column W → TeleLabel (template)

# Columns that are allowed to contain data in a "template row"
# without disqualifying it from being considered empty.
IGNORED_TEMPLATE_COLUMNS = {
    ID_INDEX,
    THUMBNAIL_IMAGE_INDEX,
    TELESAVE_INDEX,
    QUALITY_INDEX,
    TELELABEL_INDEX,
}


def build_sceneid_index(existing_rows: List[List[str]]) -> Dict[str, int]:
    """
    Build a lookup map from scene_id → sheet row number.

    - Scene ID is always read from Column C (index 2)
    - Sheet rows are 1-based, with row 1 reserved for headers
    - Rows are normalized to MAX_COLS to avoid index errors

    Args:
        existing_rows: Sheet values excluding the header row

    Returns:
        Dict[str, int]: Mapping of normalized scene_id → sheet row number
    """
    index: Dict[str, int] = {}

    for i, row in enumerate(existing_rows, start=2):
        # Normalize row length to ensure safe indexing
        row = row + [""] * (MAX_COLS - len(row))

        # Scene ID always comes from Column C
        sid = norm_scene_id(row[2])

        if sid:
            index[sid] = i

    return index


def find_empty_template_rows(existing_rows: List[List[str]]) -> List[int]:
    """
    Identify reusable template rows in the sheet.

    A row is considered a valid "empty template row" if:
    - All columns are empty EXCEPT those explicitly allowed
      in IGNORED_TEMPLATE_COLUMNS (ID, TeleSave, Quality, etc.)

    This enables:
    - Reusing pre-created template rows
    - Preserving user-entered template defaults
    - Avoiding accidental overwrites

    Args:
        existing_rows: Sheet values excluding the header row

    Returns:
        List[int]: Sheet row numbers that are safe to reuse
    """
    free: List[int] = []

    for i, row in enumerate(existing_rows, start=2):
        # Normalize row length
        row = row + [""] * (MAX_COLS - len(row))

        # Row is reusable if all non-ignored columns are empty
        if all(
            True if idx in IGNORED_TEMPLATE_COLUMNS else str(cell).strip() == ""
            for idx, cell in enumerate(row)
        ):
            free.append(i)

    return free

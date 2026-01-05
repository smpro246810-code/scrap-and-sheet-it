import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# Paths & Spreadsheet configuration
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

DATA_DIR = BASE_DIR / "google-sheets" / "data18" / "upload-scenes-to-sheet" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDENTIALS_FILE = BASE_DIR / "google-sheets" / "credentials.json"

# Target Google Spreadsheet name
SPREADSHEET_NAME = "MY PORN"


# ============================================================
# Text normalization helpers
# ============================================================


def to_title_case(value: Optional[str]) -> Optional[str]:
    """
    Convert text to Title Case while preserving:
    - Acronyms (e.g. VR, UHD)
    - Words containing digits
    - Mixed-case brand names

    Args:
        value: Input string

    Returns:
        Title-cased string or original value if invalid
    """
    if not value or not isinstance(value, str):
        return value

    value = value.replace("?", "")
    words = value.split()
    formatted_words: List[str] = []

    for word in words:
        if any(char.isdigit() for char in word):
            formatted_words.append(word)
        elif word.isupper():
            formatted_words.append(word)
        elif not (word.islower() or word.isupper()):
            formatted_words.append(word)
        else:
            formatted_words.append(word.title())

    return " ".join(formatted_words)


def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace by:
    - Replacing non-breaking spaces
    - Collapsing multiple spaces into one

    Args:
        text: Input string

    Returns:
        Cleaned string
    """
    if not isinstance(text, str):
        return text
    return " ".join(text.replace("\u00a0", " ").split())


def normalize_name(name: str) -> str:
    """
    Normalize names for comparison and lookup.

    Used for:
    - Performer matching
    - Site → network mapping

    Behavior:
    - Lowercase
    - Trim whitespace
    - Collapse internal spaces

    Args:
        name: Input name

    Returns:
        Normalized name
    """
    if not isinstance(name, str):
        return ""
    return " ".join(name.replace("\u00a0", " ").strip().lower().split())


def extract_pornstar_from_filename(json_file_path: Path) -> str:
    """
    Extract performer name from JSON filename.

    Handles known suffixes such as:
    - _scenes
    - _details
    - _scenes_and_details

    Example:
        sunny_leone_scenes.json → Sunny Leone

    Args:
        json_file_path: Path to JSON file

    Returns:
        Performer name in Title Case
    """
    filename = json_file_path.stem.lower()

    for suffix in [
        "_scenes_and_details_fixed",
        "_scenes_and_details",
        "_scenes",
        "_details",
    ]:
        if filename.endswith(suffix):
            filename = filename[: -len(suffix)]
            break

    clean_name = filename.replace("-", " ").replace("_", " ").strip()
    return " ".join(word.capitalize() for word in clean_name.split())


# ============================================================
# Network display aliases
# ============================================================

# Human-friendly display names for known networks
NETWORK_DISPLAY_ALIASES = {
    "fantasy massage": "Adult Time - Fantasy Massage",
    "girlsway": "Adult Time - Girlsway",
    "vixen media": "Vixen Media Group",
}


def apply_network_alias(name: str) -> str:
    """
    Apply a display alias to known network names.

    Args:
        name: Raw network name

    Returns:
        Aliased name if defined, otherwise original name
    """
    if not name:
        return ""
    return NETWORK_DISPLAY_ALIASES.get(
        name.strip().lower(),
        name,
    )


# ============================================================
# Duration & Scene ID helpers
# ============================================================


def convert_duration(duration: Optional[str]) -> str:
    """
    Convert duration strings into human-readable format.

    Supported formats:
    - MM:SS
    - HH:MM:SS
    - Already formatted strings (e.g. "29 min, 16 sec")

    Args:
        duration: Raw duration string

    Returns:
        Human-readable duration
    """
    if not isinstance(duration, str) or not duration.strip():
        return ""

    duration = duration.strip()
    m = re.match(r"^(\d+):(\d{2})(?::(\d{2}))?$", duration)

    if m:
        if m.group(3):
            h, m_, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return ", ".join(
                p
                for p in [
                    f"{h} hr" if h else "",
                    f"{m_} min" if m_ else "",
                    f"{s} sec" if s else "",
                ]
                if p
            )
        else:
            mins, secs = int(m.group(1)), int(m.group(2))
            hrs = mins // 60
            mins %= 60
            return ", ".join(
                p
                for p in [
                    f"{hrs} hr" if hrs else "",
                    f"{mins} min" if mins else "",
                    f"{secs} sec" if secs else "",
                ]
                if p
            )

    if re.search(r"\b(hr|min|sec)\b", duration):
        return re.sub(r"^0 hr,\s*", "", duration).strip()

    return duration


def norm_scene_id(v: Any) -> str:
    """
    Normalize scene IDs for stable matching between JSON and Sheet.

    Removes:
    - Zero-width characters
    - Non-breaking spaces
    - Extra whitespace

    Args:
        v: Raw scene ID value

    Returns:
        Normalized scene ID string
    """
    if v is None:
        return ""
    s = str(v)
    for ch in ["\u00a0", "\u200b", "\u200e", "\u200f"]:
        s = s.replace(ch, "")
    return " ".join(s.strip().split())


# ============================================================
# JSON helpers
# ============================================================


def safe_load_json(path: Path) -> Any:
    """
    Safely load a JSON file.

    - Catches file and parsing errors
    - Prints a warning instead of crashing

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON object or None on failure
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load JSON {path}: {e}")
        return None


# ============================================================
# Google Sheets helpers
# ============================================================


def make_hyperlink(url: str, text: str, enabled: bool) -> str:
    """
    Generate a Google Sheets HYPERLINK formula.

    If hyperlinks are disabled or URL is missing,
    plain text is returned instead.

    Args:
        url: Target URL
        text: Display text
        enabled: Hyperlink toggle flag

    Returns:
        Formula string or plain text
    """
    if enabled and url:
        return f'=HYPERLINK("{url.replace(chr(34), chr(34)*2)}", "{text.replace(chr(34), chr(34)*2)}")'
    return text


def get_worksheet(worksheet_name: str):
    """
    Open an existing worksheet or create a new one.

    Creation priority:
    1. Duplicate TEMPLATE sheet (if exists)
    2. Create a blank worksheet

    Args:
        worksheet_name: Target sheet name

    Returns:
        gspread Worksheet object
    """
    if not GOOGLE_CREDENTIALS_FILE.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {GOOGLE_CREDENTIALS_FILE}"
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)

    try:
        return sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            template = sh.worksheet("TEMPLATE")
            return template.duplicate(new_sheet_name=worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            return sh.add_worksheet(title=worksheet_name, rows="500", cols="25")


# ============================================================
# CLI argument parsing
# ============================================================


def parse_args():
    """
    Parse command-line arguments.

    Supported options:
    --hyperlinks on|off

    Returns:
        argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Upload scenes JSON to Google Sheets (MY PORN)."
    )
    parser.add_argument(
        "--hyperlinks",
        choices=["on", "off"],
        default="on",
        help="Enable or disable hyperlinks in Network (G) and Site (H) columns",
    )
    return parser.parse_args()

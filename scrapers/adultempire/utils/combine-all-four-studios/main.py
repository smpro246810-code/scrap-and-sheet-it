"""
AdultEmpire Studios Combiner
----------------------------

• Combines Clips, DVDs, VODs, and Blu-ray studio data
• Matches studios by title
• Produces a single normalized JSON output
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
from pathlib import Path
from typing import Dict, List, Any


# ============================================================
# PATH CONFIGURATION
# ============================================================

# Central studios-scraper data directory
STUDIOS_DATA_DIR = (
    Path(__file__).resolve().parents[2]
    / "studios-scraper"
    / "data"
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "combined-studios.json"


# ============================================================
# CATEGORY CONFIG (SINGLE SOURCE OF TRUTH)
# ============================================================

CATEGORIES: Dict[str, Dict[str, str]] = {
    "clips": {
        "file": "clips-studios.json",
        "count_key": "num_clips",
        "url_key": "clips_url",
    },
    "dvds": {
        "file": "dvds-studios.json",
        "count_key": "num_dvds",
        "url_key": "dvds_url",
    },
    "vods": {
        "file": "vods-studios.json",
        "count_key": "num_vods",
        "url_key": "vods_url",
    },
    "blurays": {
        "file": "bluray-studios.json",
        "count_key": "num_blurays",
        "url_key": "blurays_url",
    },
}


# ============================================================
# IO UTILITIES
# ============================================================

def load_json(path: Path) -> List[Dict[str, Any]]:
    """
    Safely load a JSON file.
    Returns an empty list if file does not exist.
    """
    if not path.exists():
        return []

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: List[Dict[str, Any]]):
    """
    Write JSON with consistent formatting.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ============================================================
# DATA NORMALIZATION
# ============================================================

def index_by_title(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Convert a list of studio objects into a dict keyed by title.
    """
    return {item["title"]: item for item in data if "title" in item}


def collect_all_titles(indexes: List[Dict[str, Any]]) -> List[str]:
    """
    Collect and sort all unique studio titles across categories.
    """
    titles = set()
    for index in indexes:
        titles.update(index.keys())
    return sorted(titles)


# ============================================================
# COMBINATION LOGIC
# ============================================================

def combine_studios() -> List[Dict[str, Any]]:
    """
    Merge studio data from all categories into a unified structure.
    """
    # Load and index all category data
    indexed_data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for category, cfg in CATEGORIES.items():
        file_path = STUDIOS_DATA_DIR / cfg["file"]
        data = load_json(file_path)
        indexed_data[category] = index_by_title(data)

    # Gather all unique studio titles
    all_titles = collect_all_titles(list(indexed_data.values()))

    combined: List[Dict[str, Any]] = []

    for title in all_titles:
        studio: Dict[str, Any] = {"title": title}

        for category, cfg in CATEGORIES.items():
            source = indexed_data[category].get(title)

            studio[cfg["url_key"]] = source["url"] if source else None
            studio[cfg["count_key"]] = source.get(cfg["count_key"], 0) if source else 0

        combined.append(studio)

    return combined


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    combined_data = combine_studios()
    write_json(OUTPUT_FILE, combined_data)

    print(
        f"✅ Successfully wrote {len(combined_data)} combined studios → {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()

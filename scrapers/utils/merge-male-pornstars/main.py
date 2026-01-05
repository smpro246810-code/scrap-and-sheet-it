#!/usr/bin/env python3
"""
merge-male-pornstars.py

Merge male pornstar lists from Data18 and AdultEmpire into a combined,
per-performer JSON structure that preserves each source under separate
keys (`data18` and `adultempire`). Matching is case-insensitive on the
performer's `name` field.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

# ---------------- PATHS & CONFIG ----------------
BASE_DIR = Path(__file__).resolve().parents[2]

ADULTEMPIRE_FILE = (
    BASE_DIR / "adultempire" / "pornstars-scraper" / "data" / "male-pornstars.json"
)

DATA18_FILE = BASE_DIR / "data18" / "pornstars-scraper" / "data" / "male-pornstars.json"

DATA_DIR = BASE_DIR / "utils" / "merge-male-pornstars" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "merged-male-pornstars.json"


# ---------------- HELPERS ----------------


def load_json(path: Path) -> Any:
    """Load JSON data from a file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data: List[Dict[str, Any]], path: Path) -> None:
    """Save JSON data with readable formatting."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Merged data saved to: {path}")


def strip_name(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy without the top-level `name` key."""
    d = data.copy()
    d.pop("name", None)
    return d


# ---------------- CORE MERGE LOGIC ----------------


def merge_pornstar_lists(
    data18_list: List[Dict[str, Any]],
    ae_list: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Merge two pornstar lists into combined records per performer.
    """
    combined: Dict[str, Dict[str, Any]] = {}

    # Data18 pass
    for p in data18_list:
        name = p.get("name")
        if not name:
            continue

        key = name.lower()
        combined[key] = {
            "name": name,
            "data18": strip_name(p),
            "adultempire": {},
        }

    # AdultEmpire pass
    for p in ae_list:
        name = p.get("name")
        if not name:
            continue

        key = name.lower()
        if key in combined:
            combined[key]["adultempire"] = strip_name(p)
        else:
            combined[key] = {
                "name": name,
                "data18": {},
                "adultempire": strip_name(p),
            }

    return sorted(combined.values(), key=lambda x: x["name"].lower())


# ---------------- MAIN ----------------


def main() -> None:
    print("🔍 Loading data files...")

    # AdultEmpire = plain list
    adultempire = load_json(ADULTEMPIRE_FILE)

    # Data18 = { meta, data }
    data18_payload = load_json(DATA18_FILE)
    data18 = data18_payload.get("data", [])

    print(f"• Data18 male pornstars: {len(data18)}")
    print(f"• AdultEmpire male pornstars: {len(adultempire)}")

    print("🔁 Merging performers, preserving both sources...")
    merged = merge_pornstar_lists(data18, adultempire)

    print(f"📊 Total combined performers: {len(merged)}")
    save_json(merged, OUTPUT_FILE)


if __name__ == "__main__":
    main()

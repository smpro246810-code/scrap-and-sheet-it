"""
Purpose:
--------
Extract female + trans pornstars by subtracting male pornstars
from the all-pornstars dataset.

Input Structure:
----------------
{
  "meta": {...},
  "data": [ {...}, {...} ]
}

Output Structure:
-----------------
{
  "meta": {...},
  "data": [ {...}, {...} ]
}

Notes:
------
• Comparison is done using normalized profile_url (primary)
• Falls back to normalized name if profile_url is missing
• Order from all-pornstars.json is preserved
• Meta is preserved and updated
• Safe to re-run (idempotent)
"""

from pathlib import Path
import json
from typing import Dict, List, Set
from datetime import datetime

# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "utils" / "extract-female-and-trans-pornstars" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALL_PORNSTARS_FILE = BASE_DIR / "pornstars-scraper" / "data" / "all-pornstars.json"
MALE_PORNSTARS_FILE = BASE_DIR / "pornstars-scraper" / "data" / "male-pornstars.json"
OUTPUT_FILE = DATA_DIR / "female-and-trans-pornstars.json"

# ============================================================
# HELPERS
# ============================================================


def extract_data(payload):
    """
    Accepts either:
    - a list of objects
    - or { "meta": {...}, "data": [...] }

    Returns:
        List[Dict]
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("data", [])
    raise TypeError("Unsupported JSON structure")


def normalize(text: str) -> str:
    """Normalize strings for safe comparison."""
    return " ".join(text.lower().strip().split())


def load_json(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(f"❌ File not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def extract_male_identifiers(male_data: List[Dict]) -> Set[str]:
    """
    Build a lookup set using profile_url if available,
    otherwise fallback to normalized name.
    """
    identifiers: Set[str] = set()

    for entry in male_data:
        if entry.get("profile_url"):
            identifiers.add(normalize(entry["profile_url"]))
        elif entry.get("name"):
            identifiers.add(normalize(entry["name"]))

    return identifiers


# ============================================================
# CORE LOGIC
# ============================================================


def extract_non_male_pornstars(
    all_pornstars: List[Dict],
    male_identifiers: Set[str],
) -> List[Dict]:
    """
    Remove male pornstars from all-pornstars list.
    """
    result: List[Dict] = []

    for entry in all_pornstars:
        key = None

        if entry.get("profile_url"):
            key = normalize(entry["profile_url"])
        elif entry.get("name"):
            key = normalize(entry["name"])

        if key and key not in male_identifiers:
            result.append(entry)

    return result


# ============================================================
# SAVE OUTPUT
# ============================================================


def save_json(path: Path, payload: Dict):
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"💾 Saved {len(payload['data'])} entries → {path}")


# ============================================================
# MAIN
# ============================================================


def main():
    print("📥 Loading datasets...")

    all_payload = load_json(ALL_PORNSTARS_FILE)
    male_payload = load_json(MALE_PORNSTARS_FILE)

    all_data = extract_data(all_payload)
    male_data = extract_data(male_payload)

    print(f"• All pornstars: {len(all_data)}")
    print(f"• Male pornstars: {len(male_data)}")

    male_identifiers = extract_male_identifiers(male_data)

    print("🔍 Extracting female + trans pornstars...")
    non_male_data = extract_non_male_pornstars(
        all_data,
        male_identifiers,
    )

    output_payload = {
        "meta": {
            "mode": "non-male",
            "source": "all - male",
            "total": len(non_male_data),
            "updated_at": datetime.utcnow().isoformat(),
        },
        "data": non_male_data,
    }

    save_json(OUTPUT_FILE, output_payload)
    print("🎉 Done!")


if __name__ == "__main__":
    main()

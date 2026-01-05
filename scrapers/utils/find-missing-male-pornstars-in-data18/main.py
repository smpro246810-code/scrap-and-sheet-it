import json
from pathlib import Path

# ---------------- PATHS & CONFIG ----------------
BASE_DIR = Path(__file__).resolve().parents[2]

ADULTEMPIRE_FILE = (
    BASE_DIR
    / "adultempire"
    / "pornstars-scraper"
    / "data"
    / "male-pornstars.json"
)

DATA18_FILE = (
    BASE_DIR
    / "data18"
    / "pornstars-scraper"
    / "data"
    / "male-pornstars.json"
)

DATA_DIR = (
    BASE_DIR
    / "utils"
    / "find-missing-male-pornstars-in-data18"
    / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "missing-male-pornstars-in-data18.json"


# ---------------- HELPERS ----------------

def load_json(path):
    """Load JSON data from a file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, path):
    """Save JSON data to a file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Result saved to: {path}")


def find_missing_performers(data18_list, adultempire_list):
    """
    Return performers present in AdultEmpire
    but missing in Data18 (case-insensitive).
    """
    data18_names = {p["name"].lower() for p in data18_list if p.get("name")}
    missing = [
        p for p in adultempire_list
        if p.get("name") and p["name"].lower() not in data18_names
    ]
    return missing


# ---------------- MAIN ----------------

def main():
    print("🔍 Loading data files...")

    # AdultEmpire is still a plain list
    adultempire = load_json(ADULTEMPIRE_FILE)

    # Data18 now has { meta, data }
    data18_payload = load_json(DATA18_FILE)
    data18 = data18_payload.get("data", [])

    print(f"• AdultEmpire male pornstars: {len(adultempire)}")
    print(f"• Data18 male pornstars: {len(data18)}")

    print("🔎 Comparing names (case-insensitive)...")
    missing = find_missing_performers(data18, adultempire)

    print(f"📊 Found {len(missing)} performers missing in Data18.")
    save_json(missing, OUTPUT_FILE)


if __name__ == "__main__":
    main()

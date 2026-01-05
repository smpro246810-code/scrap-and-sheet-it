import json
from difflib import SequenceMatcher
from pathlib import Path

# ---------------- PATHS ----------------
DATA_DIR = Path(__file__).resolve().parents[2] / "data"

COMBINED_FILE = DATA_DIR / "combined_adultempire_studios.json"
NETWORKS_FILE = DATA_DIR / "networks_from_sheet.json"
OUTPUT_FILE = DATA_DIR / "missing_studios_with_matches.json"
DUPLICATES_FILE = DATA_DIR / "duplicate_studios_report.json"


# ---------------- HELPERS ----------------
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_all_titles(networks_data):
    """Extract all network and site titles (flattened and normalized)."""
    titles = set()
    
    for entry in networks_data:
        if "title" in entry and entry["title"]:
            titles.add(entry["title"].lower().strip())

        if "sites" in entry:
            for site in entry["sites"]:
                if "title" in site:
                    titles.add(site["title"].lower().strip())

    return titles


def fuzzy_match(title, title_set, threshold=0.85):
    """Return titles from title_set that fuzzy match the given title."""
    matches = []
    for candidate in title_set:
        ratio = SequenceMatcher(None, title, candidate).ratio()
        if ratio >= threshold:
            matches.append({"candidate": candidate, "similarity": round(ratio, 3)})
    return matches


# ---------------- MAIN LOGIC ----------------
def find_missing_studios_with_fuzzy():
    combined_studios = load_json(COMBINED_FILE)
    networks_studios = load_json(NETWORKS_FILE)
    
    network_titles = extract_all_titles(networks_studios)
    combined_titles = {studio['title'].lower().strip(): studio for studio in combined_studios}

    missing = []
    duplicates_or_variants = {}

    for title, studio in combined_titles.items():
        if title not in network_titles:
            # Try to find fuzzy matches
            fuzzy_matches = fuzzy_match(title, network_titles)
            
            if fuzzy_matches:
                duplicates_or_variants[title] = fuzzy_matches
            else:
                missing.append(studio)

    # Save missing studios
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(missing, f, indent=2, ensure_ascii=False)

    # Save duplicate/variant report
    with open(DUPLICATES_FILE, "w", encoding="utf-8") as f:
        json.dump(duplicates_or_variants, f, indent=2, ensure_ascii=False)

    print(f"✅ Missing studios saved to: {OUTPUT_FILE}")
    print(f"✅ Variant or similarly named studios saved to: {DUPLICATES_FILE}")


if __name__ == "__main__":
    find_missing_studios_with_fuzzy()

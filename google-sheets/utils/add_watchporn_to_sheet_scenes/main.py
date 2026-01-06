"""
Scene Title Comparator (Exact + Fuzzy + Custom Matching)
-------------------------------------------------------

• Interactive CLI selection
• Exact match
• Fuzzy match (RapidFuzz)
• Custom format match via external formatter
• Logs MATCH / FUZZY MATCH / NO MATCH
"""

import json
import sys
import importlib.util
from pathlib import Path
from rapidfuzz import process, fuzz

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]

print(f"Base dir: {BASE_DIR}")

DATA_DIR = (
    BASE_DIR / "google-sheets" / "utils" / "add_watchporn_to_sheet_scenes" / "data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

SHEET_JSON = (
    BASE_DIR
    / "google-sheets"
    / "utils"
    / "extract-pornstar-scenes-from-sheet"
    / "data"
    / "peta-jensen.json"
)

WATCHPORN_JSON = (
    BASE_DIR
    / "scrapers"
    / "watchporn"
    / "pornstar-scenes-scraper"
    / "data"
    / "peta-jensen.json"
)

FUZZY_THRESHOLD = 90

# ============================================================
# CUSTOM FORMATTER (EXTERNAL FILE)
# ============================================================

CUSTOM_FORMATTER_PATH = BASE_DIR / "scrapers" / "setup" / "title-formatter" / "main.py"

spec = importlib.util.spec_from_file_location(
    "custom_formatter",
    CUSTOM_FORMATTER_PATH,
)
custom_formatter = importlib.util.module_from_spec(spec)
sys.modules["custom_formatter"] = custom_formatter
spec.loader.exec_module(custom_formatter)

# Change this if you want a different function
format_for_matching = custom_formatter.format_title

# ============================================================
# MATCH MODES
# ============================================================

MATCH_EXACT = "exact"
MATCH_FUZZY = "fuzzy"
MATCH_CUSTOM = "custom"

# ============================================================
# HELPERS
# ============================================================


def normalize(text: str) -> str:
    return " ".join(text.lower().split()) if text else ""


def canonical_watchporn_title(scene_title: str) -> str:
    if not scene_title:
        return ""

    parts = [p.strip() for p in scene_title.split(" - ")]
    if len(parts) >= 3:
        return parts[1]
    if len(parts) == 2:
        return parts[0]
    return scene_title


def prepare_title(text: str, mode: str) -> str:
    if not text:
        return ""

    if mode == MATCH_CUSTOM:
        return normalize(format_for_matching(text))

    return normalize(text)


# ============================================================
# CLI
# ============================================================


def select_matching_mode() -> str:
    print("\nSelect Matching Mode:\n")
    print("1️⃣  Exact Matching")
    print("2️⃣  Fuzzy Matching")
    print("3️⃣  Custom Format Matching\n")

    while True:
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == "1":
            return MATCH_EXACT
        if choice == "2":
            return MATCH_FUZZY
        if choice == "3":
            return MATCH_CUSTOM

        print("❌ Invalid choice. Try again.\n")


# ============================================================
# MAIN
# ============================================================


def main():
    match_mode = select_matching_mode()

    sheet_data = json.loads(SHEET_JSON.read_text(encoding="utf-8"))
    wp_data = json.loads(WATCHPORN_JSON.read_text(encoding="utf-8"))

    # Build WatchPorn lookup
    wp_titles = {}
    for scene in wp_data:
        core = canonical_watchporn_title(scene.get("scene_title", ""))
        key = prepare_title(core, match_mode)
        wp_titles[key] = scene

    wp_keys = list(wp_titles.keys())

    print(f"\n🔍 Matching mode: {match_mode.upper()}\n")

    exact_matches = 0
    fuzzy_matches = 0
    unmatched = 0

    for sheet_scene in sheet_data:
        sheet_title = sheet_scene.get("title")
        if not sheet_title:
            continue

        key = prepare_title(sheet_title, match_mode)

        # ----------------------------------------------------
        # EXACT MATCH
        # ----------------------------------------------------
        if key in wp_titles:
            scene = wp_titles[key]
            exact_matches += 1

            print("✅ MATCH")
            print(f"   Sheet title : {sheet_title}")
            print(f"   WP title    : {scene['scene_title']}")
            print(f"   URL         : {scene.get('scene_url')}\n")
            continue

        # ----------------------------------------------------
        # FUZZY MATCH (only if enabled)
        # ----------------------------------------------------
        if match_mode in {MATCH_FUZZY, MATCH_CUSTOM}:
            match, score, _ = process.extractOne(
                key,
                wp_keys,
                scorer=fuzz.token_sort_ratio,
            )

            if score >= FUZZY_THRESHOLD:
                scene = wp_titles[match]
                fuzzy_matches += 1

                print("🟡 FUZZY MATCH")
                print(f"   Sheet title : {sheet_title}")
                print(f"   WP title    : {scene['scene_title']}")
                print(f"   Score       : {score}%")
                print(f"   URL         : {scene.get('scene_url')}\n")
                continue

        # ----------------------------------------------------
        # NO MATCH
        # ----------------------------------------------------
        unmatched += 1

        # print("❌ NO MATCH")
        # print(f"   Sheet title : {sheet_title}\n")

    print("===================================")
    print(f"✅ Exact matches : {exact_matches}")
    print(f"🟡 Fuzzy matches : {fuzzy_matches}")
    print(f"❌ Non-matches  : {unmatched}")
    print(f"📦 Total        : {exact_matches + fuzzy_matches + unmatched}")
    print("===================================")


if __name__ == "__main__":
    main()

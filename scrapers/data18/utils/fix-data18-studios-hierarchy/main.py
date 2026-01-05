#!/usr/bin/env python3
"""
fix-data18-studios-hierarchy.py

Organizes Data18 studios into a parent-child hierarchy structure.

Process:
1. Loads studios JSON from studios-scraper output (flat list)
2. Identifies parent studios and substudios based on URL depth
3. Groups substudios under their parent studios
4. Separates unrelated studios into an "Others" group
5. Saves hierarchical JSON with parent + sites array structure

This transforms a flat studio list into a nested hierarchy for better
data organization and presentation in downstream applications.
"""

import json
from pathlib import Path

# ===== FILE PATHS =====
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "studios-scraper" / "data" / "data18-studios.json"
DATA_DIR = BASE_DIR / "utils" / "fix-data18-studios-hierarchy" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = DATA_DIR / "fixed-data18-studios.json"

# ===== HELPER FUNCTION =====
def get_parent_url(url: str) -> str:
    """Extract parent studio URL from a substudio URL.
    
    Data18 uses URL hierarchy like: /studios/parent_studio/substudio/
    This extracts the parent_studio portion to group related studios.
    
    Args:
        url: Full URL of a substudio (e.g., https://www.data18.com/studios/parent/child/).
    
    Returns:
        Parent studio URL (e.g., https://www.data18.com/studios/parent/).
    """
    parts = url.split("/studios/")[-1].split("/")
    return f"https://www.data18.com/studios/{parts[0]}"


# ===== LOAD INPUT DATA =====
# Read flat studios list from studios-scraper output
try:
    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        studios = json.load(file)
except FileNotFoundError:
    print(f"❌ data18_studios.json file not found at: {INPUT_FILE}")
    exit()

# ===== ORGANIZE INTO HIERARCHY =====
# Dictionary to store grouped studios: {parent_url: {"parent": studio_dict, "children": [list]}}
grouped_studios = {}
others = []  # Studios that don't fit parent-child structure

# Categorize studios: identify parent studios (1-level URL) vs substudios (2+ levels)
for studio in studios:
    url = studio["url"]
    if url.count("/") > 4:  # Multiple path segments indicate a substudio (parent/child structure)
        parent_url = get_parent_url(url)  # Extract parent URL
        if parent_url not in grouped_studios:
            grouped_studios[parent_url] = {
                "parent": None,
                "children": []
            }
        grouped_studios[parent_url]["children"].append(studio)  # Add substudio to parent's children
    else:
        # Single-level URL indicates a parent/independent studio
        parent_url = url
        if parent_url not in grouped_studios:
            grouped_studios[parent_url] = {
                "parent": studio,
                "children": []
            }
        else:
            # Update parent info if new entry for existing parent
            grouped_studios[parent_url]["parent"] = studio

# ===== PREPARE HIERARCHICAL OUTPUT =====
# Transform grouped data into final output format with parent + sites structure
output_data = []

for parent_url, data in grouped_studios.items():
    parent = data.get("parent")
    children = data["children"]

    if parent and children:
        # CASE 1: Parent with substudios - create hierarchy entry
        entry = {
            "title": parent["title"],
            "url": parent["url"],
            "num_scenes": parent["num_scenes"],
            "sites": children  # Array of substudio objects
        }
        output_data.append(entry)
    else:
        # CASE 2: Studios without complete parent-child relationship
        if parent:  # Parent exists but has no substudios - treat as standalone
            others.append({
                "title": parent["title"],
                "url": parent["url"],
                "num_scenes": parent["num_scenes"]
            })
        else:
            # No parent found (orphaned substudios) - treat as standalone
            others.extend(children)

# Add "Others" group if any unrelated studios exist
if others:
    output_data.append({
        "title": "Others",
        "url": None,
        "num_scenes": None,
        "sites": others  # Catchall for studios that don't fit hierarchy
    })

# ===== WRITE HIERARCHICAL OUTPUT =====
# Save to JSON with formatting for readability
with open(OUTPUT_FILE, "w", encoding="utf-8") as output_file:
    json.dump(output_data, output_file, indent=2, ensure_ascii=False)

print(f"✅ Grouped studios JSON saved to: {OUTPUT_FILE}")
print(f"📊 Total hierarchies: {len(output_data)}")
print(f"📝 Input studios: {len(studios)}")


import base64
import json
import requests

# -----------------------------
# CONFIG
# -----------------------------
STASH_URL = "http://localhost:9999/graphql"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJNci4gV2hpdGUiLCJzdWIiOiJBUElLZXkiLCJpYXQiOjE3NjM3NTg0NDN9.gaPhXh3QKIvJCW_JNygGZwlAOdzWFrRvgaQnbkWxEWU"

# JSON file created from your Google Sheet:
JSON_FILE = r"E:\data18_scraper\data\pornstars_from_sheet.json"

# -----------------------------
# GRAPHQL QUERIES
# -----------------------------
SEARCH_PERFORMER = """
query PerformerSearch($name: String!) {
  findPerformers(performer_filter: {name: {value: $name, modifier: EQUALS}}) {
    performers {
      id
      name
    }
  }
}
"""

UPDATE_IMAGE = """
mutation PerformerUpdate($id: ID!, $image: String!) {
  performerUpdate(input: {id: $id, image: $image}) {
    id
    name
  }
}
"""

# -----------------------------
# GRAPHQL REQUEST WRAPPER
# -----------------------------
def gql(query, variables):
    headers = {
        "ApiKey": API_KEY,
        "Accept": "application/json",
    }
    resp = requests.post(STASH_URL, json={"query": query, "variables": variables}, headers=headers)
    return resp.json()

# -----------------------------
# MAIN LOGIC
# -----------------------------
print("Starting performer image assignment using pornstar JSON...\n")

# Load JSON: list of { "name": "...", "image": "..." }
with open(JSON_FILE, "r", encoding="utf-8") as f:
    pornstar_list = json.load(f)

for entry in pornstar_list:
    performer_name = entry.get("name")
    image_url = entry.get("image")

    print(f"Processing: {performer_name}")

    # If image is null, skip
    if not image_url:
        print("  -> No image provided in JSON. Skipping performer.")
        continue

    # Search for performer in Stash
    data = gql(SEARCH_PERFORMER, {"name": performer_name})

    performers = data["data"]["findPerformers"]["performers"]
    if not performers:
        print("  -> Performer NOT FOUND in Stash.")
        continue

    performer_id = performers[0]["id"]
    print(f"  -> Found performer ID: {performer_id}")

    # Download image from URL
    try:
        img_response = requests.get(image_url, timeout=10)
        img_response.raise_for_status()
        img_bytes = img_response.content
    except Exception as e:
        print(f"  -> ERROR downloading image: {e}")
        continue

    # Convert to base64
    b64_image = base64.b64encode(img_bytes).decode("utf-8")
    image_string = "data:image/jpeg;base64," + b64_image

    # Upload to Stash
    result = gql(UPDATE_IMAGE, {"id": performer_id, "image": image_string})

    if "errors" in result:
        print("  -> ERROR updating performer:", result["errors"])
    else:
        print("  -> Image successfully updated!")

print("\nDone!")

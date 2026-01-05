import requests
import json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

GRAPHQL_URL = "https://stashdb.org/graphql"

# IMPORTANT — replace with your cookie
COOKIE_HEADER = "stashbox=MTc2NDA2NzcwNnxEWDhFQVFMX2dBQUJFQUVRQUFCRV80QUFBUVp6ZEhKcGJtY01DQUFHZFhObGNrbEVCbk4wY21sdVp3d21BQ1F3TVRsaFlUQm1NeTAxT0dVNUxUY3dZekl0T0Rjd1l5MDFPV0k0WmpZNE5XWXdZVEU9fBnHULziaB2zCvEfFnCe0DdfIxv1k5Z9WSKrKnD5DANU"

OUTPUT_JSON = ROOT_DIR / "stash" / "studios_list.json"

# ---------------------------------------------------------
# GraphQL Query for Studios
# ---------------------------------------------------------

QUERY = """
query Studios($input: StudioQueryInput!) {
  queryStudios(input: $input) {
    count
    studios {
      id
      name
      aliases
      deleted
      parent {
        id
        name
      }
      urls {
        url
        site {
          id
          name
          icon

        }
      }
      images {
        id
        url
        width
        height
      }
      is_favorite
    }
  }
}
"""

def fetch_studios():
    headers = {
        "Cookie": COOKIE_HEADER,
        "Content-Type": "application/json",
    }

    studios = []
    page = 1
    per_page = 40

    while True:
        query_vars = {
            "input": {
                "names": "",
                "page": page,
                "per_page": per_page,
                "sort": "NAME",
                "direction": "ASC"
            }
        }

        response = requests.post(
            GRAPHQL_URL,
            json={"query": QUERY, "variables": query_vars},
            headers=headers
        )

        data = response.json()

        # GraphQL errors (auth, permissions, etc.)
        if "errors" in data:
            print("ERROR:", data["errors"])
            return

        studio_data = data["data"]["queryStudios"]
        studios.extend(studio_data["studios"])

        print(f"Fetched page {page}: {len(studio_data['studios'])} studios")

        if len(studio_data["studios"]) < per_page:
            break

        page += 1

    print(f"\nTotal studios fetched: {len(studios)}")

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(studios, f, indent=2, ensure_ascii=False)

    print(f"\nSaved → {OUTPUT_JSON}")

def fetch_studios_page1():
    headers = {
        "Cookie": COOKIE_HEADER,
        "Content-Type": "application/json",
    }

    query_vars = {
        "input": {
            "names": "",
            "page": 1,
            "per_page": 40,
            "sort": "NAME",
            "direction": "ASC"
        }
    }

    response = requests.post(
        GRAPHQL_URL,
        json={"query": QUERY, "variables": query_vars},
        headers=headers
    )

    data = response.json()

    # Handle GraphQL errors
    if "errors" in data:
        print("ERROR:", data["errors"])
        return

    studio_data = data["data"]["queryStudios"]
    studios = studio_data["studios"]

    print(f"Fetched page 1: {len(studios)} studios")

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(studios, f, indent=2, ensure_ascii=False)

    print(f"\nSaved → {OUTPUT_JSON}")


if __name__ == "__main__":
    fetch_studios()

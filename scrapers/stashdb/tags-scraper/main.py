import requests
import json
from pathlib import Path

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

GRAPHQL_URL = "https://stashdb.org/graphql"
COOKIE_HEADER = "stashbox=MTc2NDE1MDQ5MXxEWDhFQVFMX2dBQUJFQUVRQUFCRV80QUFBUVp6ZEhKcGJtY01DQUFHZFhObGNrbEVCbk4wY21sdVp3d21BQ1F3TVRsaFlUQm1NeTAxT0dVNUxUY3dZekl0T0Rjd1l5MDFPV0k0WmpZNE5XWXdZVEU9fCdDZYcKhopnxO4osfXOwOfFmDbRqCa5EHNX7uJJp5kY"


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_JSON = ROOT_DIR / "stash" / "tag_categories_with_tags.json"

# -----------------
# TEST LIMITS
# -----------------
GROUP_LIMIT = 2  # number of groups (ACTION, PEOPLE, SCENE …)
CATEGORY_LIMIT = 2  # number of categories per group
TAG_LIMIT = 2  # number of tags per category

# ---------------------------------------------------------
# QUERIES
# ---------------------------------------------------------

QUERY_CATEGORIES = """
query Categories {
  queryTagCategories {
    count
    tag_categories {
      id
      name
      description
      group
    }
  }
}
"""

QUERY_TAGS = """
query Tags($input: TagQueryInput!) {
  queryTags(input: $input) {
    count
    tags {
      id
      name
      description
      aliases
    }
  }
}
"""

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------


def gql(query, variables=None):
    headers = {"Cookie": COOKIE_HEADER, "Content-Type": "application/json"}
    resp = requests.post(
        GRAPHQL_URL, json={"query": query, "variables": variables}, headers=headers
    )
    data = resp.json()
    if "errors" in data:
        print("GraphQL ERROR:", data["errors"])
        return None
    return data["data"]


# ---------------------------------------------------------
# FETCH TAGS FOR ONE CATEGORY
# ---------------------------------------------------------


def fetch_tags_for_category(category_id, tag_limit=None):
    tags = []
    page = 1
    per_page = 100

    while True:
        vars = {
            "input": {
                "category_id": category_id,
                "page": page,
                "per_page": per_page,
                "sort": "NAME",
                "direction": "ASC",
            }
        }

        res = gql(QUERY_TAGS, vars)
        if not res:
            break

        block = res["queryTags"]["tags"]
        tags.extend(block)

        if len(block) < per_page:
            break

        page += 1

    # apply tag limit
    if tag_limit is not None:
        tags = tags[:tag_limit]

    return tags


# ---------------------------------------------------------
# MAIN FETCH LOGIC
# ---------------------------------------------------------


def fetch_all():
    print("Fetching categories...")

    raw = gql(QUERY_CATEGORIES)
    categories = raw["queryTagCategories"]["tag_categories"]

    # Group categories by .group
    grouped = {}
    for cat in categories:
        grouped.setdefault(cat["group"], []).append(cat)

    # Apply GROUP_LIMIT
    groups = list(grouped.keys())[:GROUP_LIMIT]

    result = {}

    for group in groups:
        group_list = grouped[group][:CATEGORY_LIMIT]  # limit categories
        result[group] = []

        for cat in group_list:
            print(f"  Fetching tags for category: {cat['name']}")

            tags = fetch_tags_for_category(cat["id"], TAG_LIMIT)

            result[group].append(
                {
                    "id": cat["id"],
                    "name": cat["name"],
                    "description": cat["description"],
                    "tags": tags,
                }
            )

    # Save output
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nSaved → {OUTPUT_JSON}")


# ---------------------------------------------------------
# RUN
# ---------------------------------------------------------

if __name__ == "__main__":
    fetch_all()

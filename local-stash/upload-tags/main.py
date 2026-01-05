#!/usr/bin/env python3
"""
upload_tags_to_local.py

Upload tag-groups -> categories -> tags to local Stash.

Input JSON (default): ../stash/tag_categories_with_tags.json

Behavior:
- Creates/fetches a group tag named "Group - <GROUPNAME>"
- Creates/fetches a category tag named "Category - <CATEGORYNAME>" with parent -> group tag
- Creates/updates each tag (name stays e.g. "ALS Rocket"), sets parent_ids -> [category_tag_id]
- Stores description only when present.
- If an image file matches the category name (filename without ext), that image is uploaded to the category tag.
- Retries when server rejects unknown fields (e.g. stash_ids)
"""

import json
import time
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import mimetypes
import base64
import os
import re

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_FILE = SCRIPT_DIR / "upload_tags_to_local.log"

# Local Stash GraphQL endpoint (change if needed)
GRAPHQL_URL = "http://localhost:9999/graphql"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJNci4gV2hpdGUiLCJzdWIiOiJBUElLZXkiLCJpYXQiOjE3NjM3NTg0NDN9.gaPhXh3QKIvJCW_JNygGZwlAOdzWFrRvgaQnbkWxEWU"
HEADERS = {
    "ApiKey": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Input JSON (produced earlier by your fetch script)
INPUT_JSON = SCRIPT_DIR.parent / "stash" / "tag_categories_with_tags.json"  # ../stash/...
OUTPUT_REPORT = SCRIPT_DIR / "upload_tags_report.json"

# Directory containing images to map to categories.
# If a category name "Accessories" and file "accessories.jpg" exists here,
# that image will be uploaded to the category tag.
IMAGE_DIR = SCRIPT_DIR / "category_images"

# TEST LIMITS (set small for testing). None => no limit
CATEGORY_LIMIT = None  # e.g. 2
TAG_LIMIT = None       # e.g. 5

# Allowed image file extensions
IMAGE_EXTS = [".png", ".jpg", ".jpeg", ".webp", ".gif"]

# ---------------- Logging utilities ----------------
def _log_to_file(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{ts} {msg}\n")
    except Exception:
        pass

def info(msg: str):
    print(msg)
    _log_to_file("[INFO] " + msg)

def success(msg: str):
    print(msg)
    _log_to_file("[SUCCESS] " + msg)

def warn(msg: str):
    print(msg)
    _log_to_file("[WARNING] " + msg)

def error(msg: str):
    print(msg)
    _log_to_file("[ERROR] " + msg)

# ---------------- GraphQL queries / mutations ----------------
FIND_TAGS = """
query FindTags($name: String!) {
  findTags(tag_filter: { name: { value: $name, modifier: EQUALS } }) {
    tags { id name }
  }
}
"""

TAG_CREATE = """
mutation TagCreate($input: TagCreateInput!) {
  tagCreate(input: $input) {
    id
    name
  }
}
"""

TAG_UPDATE = """
mutation TagUpdate($input: TagUpdateInput!) {
  tagUpdate(input: $input) {
    id
    name
  }
}
"""

# ---------------- GraphQL helper ----------------
def gql(query: str, variables: dict, headers: dict = HEADERS, timeout: int = 30) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables}, headers=headers, timeout=timeout)
    except Exception as e:
        error(f"HTTP request failed: {e}")
        return None
    try:
        return resp.json()
    except Exception:
        error(f"Non-JSON response (HTTP {resp.status_code}): {resp.text}")
        return None

# ---------------- Image helpers ----------------
def find_image_for_name(name: str, image_dir: Path) -> Optional[Path]:
    """Case-insensitive match of filename (without ext) to name. Returns first match Path or None."""
    if not image_dir or not image_dir.exists():
        return None
    target = re.sub(r"\s+", "-", name.strip().lower())  # normalize spaces -> dashes
    # also try plain lowered name variant
    variants = {name.strip().lower(), target}
    for f in image_dir.iterdir():
        if not f.is_file():
            continue
        ext = f.suffix.lower()
        if ext not in IMAGE_EXTS:
            continue
        fname = f.stem.lower()
        if fname in variants:
            return f
    # fallback: try startswith or contains (less strict) - optional
    for f in image_dir.iterdir():
        if not f.is_file():
            continue
        ext = f.suffix.lower()
        if ext not in IMAGE_EXTS:
            continue
        fname = f.stem.lower()
        for v in variants:
            if v == fname or fname.startswith(v) or v.startswith(fname) or v in fname:
                return f
    return None

def file_to_data_url(path: Path) -> Optional[str]:
    try:
        mime, _ = mimetypes.guess_type(str(path))
        if not mime:
            mime = "application/octet-stream"
        b = path.read_bytes()
        b64 = base64.b64encode(b).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception as e:
        warn(f"Failed to convert image {path} to data URL: {e}")
        return None

# ---------------- Tag helpers ----------------
def find_tag_by_name(name: str) -> Optional[Dict[str, Any]]:
    res = gql(FIND_TAGS, {"name": name})
    if not res:
        return None
    if "errors" in res:
        warn(f"GraphQL errors when searching tag '{name}': {res['errors']}")
        return None
    tags = res.get("data", {}).get("findTags", {}).get("tags", [])
    return tags[0] if tags else None

def _contains_unknown_field_error(resp: Dict[str, Any], field_name: str) -> bool:
    if not resp or "errors" not in resp:
        return False
    js = json.dumps(resp["errors"]).lower()
    return field_name.lower() in js or "unknown field" in js

def create_tag_with_retry(payload: dict) -> Optional[Dict[str, Any]]:
    """Attempt to create; if server complains about stash_ids or image, retry without them."""
    res = gql(TAG_CREATE, {"input": payload})
    if res is None:
        return None
    if "errors" in res:
        # retry logic for stash_ids or other unknown fields
        # try removing stash_ids first
        if _contains_unknown_field_error(res, "stash_ids") and "stash_ids" in payload:
            payload2 = {k: v for k, v in payload.items() if k != "stash_ids"}
            warn(f"Server rejected 'stash_ids' on create for '{payload.get('name')}', retrying without stash_ids")
            res2 = gql(TAG_CREATE, {"input": payload2})
            if res2 and "errors" not in res2 and res2.get("data", {}).get("tagCreate"):
                return res2["data"]["tagCreate"]
        # try removing image if server rejects it
        if _contains_unknown_field_error(res, "image") and "image" in payload:
            payload2 = {k: v for k, v in payload.items() if k != "image"}
            warn(f"Server rejected 'image' on create for '{payload.get('name')}', retrying without image")
            res2 = gql(TAG_CREATE, {"input": payload2})
            if res2 and "errors" not in res2 and res2.get("data", {}).get("tagCreate"):
                return res2["data"]["tagCreate"]
        error(f"GraphQL create errors for '{payload.get('name')}' -> {res['errors']}")
        return None
    return res.get("data", {}).get("tagCreate")

def update_tag_with_retry(payload: dict) -> Optional[Dict[str, Any]]:
    res = gql(TAG_UPDATE, {"input": payload})
    if res is None:
        return None
    if "errors" in res:
        # remove stash_ids if complained
        if _contains_unknown_field_error(res, "stash_ids") and "stash_ids" in payload:
            payload2 = {k: v for k, v in payload.items() if k != "stash_ids"}
            warn(f"Server rejected 'stash_ids' on update for '{payload.get('name')}', retrying without stash_ids")
            res2 = gql(TAG_UPDATE, {"input": payload2})
            if res2 and "errors" not in res2 and res2.get("data", {}).get("tagUpdate"):
                return res2["data"]["tagUpdate"]
        # remove image if complained
        if _contains_unknown_field_error(res, "image") and "image" in payload:
            payload2 = {k: v for k, v in payload.items() if k != "image"}
            warn(f"Server rejected 'image' on update for '{payload.get('name')}', retrying without image")
            res2 = gql(TAG_UPDATE, {"input": payload2})
            if res2 and "errors" not in res2 and res2.get("data", {}).get("tagUpdate"):
                return res2["data"]["tagUpdate"]
        error(f"GraphQL update errors for '{payload.get('name')}' -> {res['errors']}")
        return None
    return res.get("data", {}).get("tagUpdate")

def ensure_tag(name: str, description: Optional[str] = None, parent_ids: Optional[List[str]] = None, stash_ids: Optional[List[dict]] = None, image_data_url: Optional[str] = None) -> Optional[str]:
    """
    Ensure tag exists: find by exact name -> update (set description/parent_ids/image) or create.
    Returns local tag id or None on failure.
    """
    found = find_tag_by_name(name)
    if found:
        t_id = found["id"]
        upd: Dict[str, Any] = {"id": t_id}
        if description:
            upd["description"] = description
        if parent_ids:
            upd["parent_ids"] = [pid for pid in parent_ids if pid]
        if stash_ids:
            upd["stash_ids"] = stash_ids
        if image_data_url:
            upd["image"] = image_data_url
        res = update_tag_with_retry(upd)
        if res:
            success(f"Updated tag: {name}")
            return res["id"]
        else:
            warn(f"Update failed for tag: {name}")
            return None
    else:
        create_payload: Dict[str, Any] = {"name": name}
        if description:
            create_payload["description"] = description
        if parent_ids:
            create_payload["parent_ids"] = [pid for pid in parent_ids if pid]
        if stash_ids:
            create_payload["stash_ids"] = stash_ids
        if image_data_url:
            create_payload["image"] = image_data_url
        res = create_tag_with_retry(create_payload)
        if res:
            success(f"Created tag: {name} (id={res['id']})")
            return res["id"]
        else:
            warn(f"Create failed for tag: {name}")
            return None

# ---------------- Main flow ----------------
def main():
    if not INPUT_JSON.exists():
        error(f"Input JSON not found: {INPUT_JSON}")
        return

    try:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        error(f"Failed to load input JSON: {e}")
        return

    if not isinstance(data, dict):
        error("Input JSON must be an object mapping group -> categories list.")
        return

    groups = list(data.keys())
    info(f"Loaded groups: {len(groups)}")

    report = {"groups": []}

    for group_name in groups:
        # Group header exactly as requested
        group_display = f"Group - {group_name}"
        info(group_display)

        # Create/find group tag (no description by default)
        group_tag_id = ensure_tag(group_display, description=None, parent_ids=None, stash_ids=None, image_data_url=None)
        if not group_tag_id:
            warn(f"Create/find failed for group tag '{group_display}' - continuing (children may fail).")

        categories = data.get(group_name) or []
        if CATEGORY_LIMIT:
            categories = categories[:CATEGORY_LIMIT]
        info(f"  Processing {len(categories)} categories for group {group_name}")

        group_report = {"group": group_name, "group_tag_id": group_tag_id, "categories": []}

        for cat in categories:
            # category may be dict or string
            cat_name = cat.get("name") if isinstance(cat, dict) else str(cat)
            if not cat_name:
                continue
            cat_display = f"Category - {cat_name}"
            info(f"  {cat_display}")   # EXACT format required

            # optional description
            cat_desc = None
            if isinstance(cat, dict):
                cat_desc = cat.get("description") or None

            # map image if available
            image_path = find_image_for_name(cat_name, IMAGE_DIR)
            image_data_url = file_to_data_url(image_path) if image_path else None
            if image_path:
                info(f"    Found image for category '{cat_name}': {image_path.name}")

            # create/find category tag with parent -> group_tag_id
            parent_ids = [group_tag_id] if group_tag_id else None
            category_tag_id = ensure_tag(cat_display, description=cat_desc, parent_ids=parent_ids, stash_ids=None, image_data_url=image_data_url)
            if not category_tag_id:
                warn(f"Create/find failed for category tag '{cat_display}'.")
            cat_report = {"category": cat_name, "category_tag_id": category_tag_id, "tags": []}

            # iterate tags inside category
            tag_list = []
            if isinstance(cat, dict):
                tag_list = cat.get("tags") or []
            # normalize tags list: dicts or strings
            normalized_tags = []
            for t in tag_list:
                if isinstance(t, dict):
                    normalized_tags.append({"name": t.get("name"), "description": t.get("description")})
                else:
                    normalized_tags.append({"name": str(t), "description": None})
            if TAG_LIMIT:
                normalized_tags = normalized_tags[:TAG_LIMIT]

            for t in normalized_tags:
                tname = t.get("name")
                if not tname:
                    continue
                # print tag name exactly as requested (no prefix)
                info(f"    {tname}")
                tdesc = t.get("description") or None
                # set parent to category tag
                pids = [category_tag_id] if category_tag_id else None
                # create/update tag (no image mapping for individual tags by default)
                tag_id = ensure_tag(tname, description=tdesc, parent_ids=pids, stash_ids=None, image_data_url=None)
                cat_report["tags"].append({"name": tname, "id": tag_id})

            group_report["categories"].append(cat_report)

        report["groups"].append(group_report)

    # save report
    try:
        with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        success(f"Report saved → {OUTPUT_REPORT}")
    except Exception as e:
        warn(f"Could not save report: {e}")

    success("ALL TAGS UPLOADED (attempted).")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Import studios from stash/studios_list.json into local Stash.

- Local Stash (STASH_URL) used for create/update.
- StashDB (STASHBOX_URL) used to fetch full parent details.
- Uses single 'url' (first found) for payload but stores all remote URLs in 'details'.
- Adds stash_ids entries with structure: { "stash_id": "...", "endpoint": "https://stashdb.org/graphql" }
- If remote studio has is_favorite True, sets favorite=True locally.
- Logs to import_studios_to_local.log in the same directory as this script.
"""

import requests
import json
import os
import base64
import mimetypes
import time
from pathlib import Path
from urllib.parse import urlparse

# ----------------------------
# Paths & configuration
# ----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_FILE = SCRIPT_DIR / "import_studios_to_local.log"

ROOT_DIR = Path(__file__).resolve().parents[1]
JSON_FILE = ROOT_DIR / "stash" / "studios_list.json"

# Resume imports starting from this letter (case-insensitive)
RESUME_FROM_LETTER = "R"

# Local Stash (used for create/update)
STASH_URL = "http://localhost:9999/graphql"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJNci4gV2hpdGUiLCJzdWIiOiJBUElLZXkiLCJpYXQiOjE3NjM3NTg0NDN9.gaPhXh3QKIvJCW_JNygGZwlAOdzWFrRvgaQnbkWxEWU"

# StashDB / Stash-Box (official stashdb server) used to fetch full parent details
STASHBOX_URL = "https://stashdb.org/graphql"
STASHBOX_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIwMTlhYTBmMy01OGU5LTcwYzItODcwYy01OWI4ZjY4NWYwYTEiLCJzdWIiOiJBUElLZXkiLCJpYXQiOjE3NjM2MzY4OTN9.9DsQLs8p9mxUCragDuKCgbpMULoFvWl9FTXJe6PGAS0"

TMP_DIR = Path("/tmp/studio_import")
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Use uploaded sample image fallback (we use an uploaded path recorded earlier)
SAMPLE_UPLOADED_IMAGE = "/mnt/data/cfca7b01-690e-4b73-93e4-6838b9943593.png"
USE_UPLOADED_SAMPLE_IMAGE = False

HEADERS = {"ApiKey": API_KEY, "Accept": "application/json"}
HEADERS_STASHBOX = {"ApiKey": STASHBOX_KEY, "Accept": "application/json"}

# -----------------------
# Logging (color + file)
# -----------------------
from colorama import Fore, Style, init
init(autoreset=True)

def log(message: str, level: str = "info", print_console: bool = True):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    tag = f"[{level.upper()}]"
    colors = {
        "info": Fore.CYAN + Style.BRIGHT,
        "success": Fore.GREEN + Style.BRIGHT,
        "warning": Fore.YELLOW + Style.BRIGHT,
        "error": Fore.LIGHTRED_EX + Style.BRIGHT,
    }
    color = colors.get(level, Fore.WHITE)
    if print_console:
        try:
            print(f"{color}{timestamp} {tag:<10}{Style.RESET_ALL} {message}")
        except Exception:
            print(f"{timestamp} {tag} {message}")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} {tag} {message}\n")
    except Exception as e:
        print(f"Failed to write log file: {e}")

class LoggerAdapter:
    def info(self, msg): log(msg, "info")
    def success(self, msg): log(msg, "success")
    def warning(self, msg): log(msg, "warning")
    def error(self, msg): log(msg, "error")

logger = LoggerAdapter()

# -----------------------
# GraphQL queries/mutations
# -----------------------
FIND_STUDIO_FULL = """
fragment URLFragment on URL {
  url
  type
  site { id name icon }
}

fragment ImageFragment on Image {
  id
  url
  width
  height
}

fragment StudioFragment on Studio {
  id
  name
  aliases
  urls {
    ...URLFragment
  }
  parent {
    id
    name
  }
  images {
    ...ImageFragment
  }
  is_favorite
}

query FindStudioFull($name: String!) {
  findStudio(name: $name) {
    ...StudioFragment
  }
}
"""

CREATE_STUDIO = """
mutation CreateStudio($input: StudioCreateInput!) {
  studioCreate(input: $input) {
    id
    name
  }
}
"""

UPDATE_STUDIO = """
mutation UpdateStudio($input: StudioUpdateInput!) {
  studioUpdate(input: $input) {
    id
    name
  }
}
"""

FIND_TAG = """
query FindTags($name: String!) {
  findTags(tag_filter: { name: { value: $name, modifier: EQUALS } }) {
    tags { id name }
  }
}
"""

CREATE_TAG = """
mutation CreateTag($input: TagCreateInput!) {
  tagCreate(input: $input) {
    id
    name
  }
}
"""

# -----------------------
# Helpers
# -----------------------
def gql(query, variables=None, use_stashbox=False):
    url = STASHBOX_URL if use_stashbox else STASH_URL
    headers = HEADERS_STASHBOX if use_stashbox else HEADERS
    payload = {"query": query, "variables": variables}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
    except Exception as e:
        logger.error(f"HTTP REQUEST FAILED ({'stashbox' if use_stashbox else 'local'}): {e}")
        return None
    if resp.status_code != 200:
        logger.error(f"HTTP ERROR: {resp.status_code} ({'stashbox' if use_stashbox else 'local'}) - {resp.text}")
        return None
    try:
        return resp.json()
    except Exception:
        logger.error(f"Failed to parse JSON response: {resp.text}")
        return None

def guess_mime_from_url(url):
    path = urlparse(url).path
    mime, _ = mimetypes.guess_type(path)
    return mime or "application/octet-stream"

def download_to_data_url(url, dest_folder=TMP_DIR):
    try:
        r = requests.get(url, timeout=30, stream=True)
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"download failed: {url} -> {e}")
        return None
    mime = r.headers.get("Content-Type") or guess_mime_from_url(url)
    content = r.content
    b64 = base64.b64encode(content).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"
    filename = os.path.basename(urlparse(url).path) or "image"
    out_path = Path(dest_folder) / f"download_{filename}"
    try:
        with open(out_path, "wb") as f:
            f.write(content)
        logger.info(f"Saved remote image to {out_path}")
    except Exception:
        logger.warning(f"Could not save remote image to {out_path}")
    return data_url

def file_to_data_url(local_path):
    local = Path(local_path)
    if not local.exists():
        return None
    mime, _ = mimetypes.guess_type(str(local))
    if not mime:
        mime = "application/octet-stream"
    with open(local, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

# -----------------------
# Tag helpers
# -----------------------
tag_cache = {}
def get_or_create_tag(name):
    if not name:
        return None
    if name in tag_cache:
        return tag_cache[name]
    res = gql(FIND_TAG, {"name": name}, use_stashbox=False)
    tag_id = None
    if res and res.get("data") and res["data"].get("findTags") and res["data"]["findTags"].get("tags"):
        tags = res["data"]["findTags"]["tags"]
        if tags:
            tag_id = tags[0]["id"]
    else:
        created = gql(CREATE_TAG, {"input": {"name": name}}, use_stashbox=False)
        if created and created.get("data") and "errors" not in created:
            tag_id = created["data"]["tagCreate"]["id"]
        else:
            logger.error(f"Failed to create tag: {name} -> {created}")
            tag_id = None
    tag_cache[name] = tag_id
    return tag_id

# -----------------------
# Studio helpers
# -----------------------
def find_studio_by_name(name):
    """Query local Stash for a studio by exact name and safely return first studio or None."""
    query = """
    query LocalFindStudio($name: String!) {
      findStudios(studio_filter: { name: { value: $name, modifier: EQUALS } }) {
        count
        studios {
          id
          name
        }
      }
    }
    """
    res = gql(query, {"name": name}, use_stashbox=False)
    if not res or "data" not in res:
        return None
    fs = res["data"].get("findStudios")
    if not fs or not fs.get("studios"):
        return None
    studios = fs["studios"]
    if not studios:
        return None
    return studios[0]

def fetch_full_studio(name, use_stashbox=True):
    res = gql(FIND_STUDIO_FULL, {"name": name}, use_stashbox=use_stashbox)
    if not res or "data" not in res:
        return None
    return res["data"].get("findStudio")

def _pick_first_url_from_urlobjs(url_objs):
    if not url_objs:
        return None
    for u in url_objs:
        if isinstance(u, dict):
            url_val = u.get("url")
            if url_val:
                return url_val
        elif isinstance(u, str):
            return u
    return None

def _collect_all_urls_from_urlobjs(url_objs):
    out = []
    if not url_objs:
        return out
    for u in url_objs:
        if isinstance(u, dict):
            url_val = u.get("url")
            if url_val:
                out.append(url_val)
        elif isinstance(u, str):
            out.append(u)
    return out

def _pick_first_image_url(images):
    if not images:
        return None
    for img in images:
        if isinstance(img, dict) and img.get("url"):
            return img.get("url")
    return None

def create_studio_payload(st, remote_full=None):
    # main single url preference: source JSON first, then remote_full
    chosen_url = None
    src_urls = st.get("urls") or []
    if src_urls:
        chosen_url = _pick_first_url_from_urlobjs(src_urls)
    elif remote_full and remote_full.get("urls"):
        chosen_url = _pick_first_url_from_urlobjs(remote_full.get("urls"))

    # details: original details + All URLs block (source + remote)
    details_text = st.get("details") or ""
    all_urls = []
    all_urls.extend(_collect_all_urls_from_urlobjs(src_urls))
    if remote_full:
        # append remote urls not already present
        for ru in _collect_all_urls_from_urlobjs(remote_full.get("urls") or []):
            if ru not in all_urls:
                all_urls.append(ru)
    if all_urls:
        details_text = (details_text + "\n\nAll URLs:\n" + "\n".join(f"- {u}" for u in all_urls)).strip()

    # tags
    tag_ids = []
    for t in st.get("tags", []):
        tname = t.get("name") if isinstance(t, dict) else t
        tid = get_or_create_tag(tname)
        if tid:
            tag_ids.append(tid)

    # image -> data URL
    image_data_url = None
    if st.get("images"):
        img = _pick_first_image_url(st.get("images"))
        if img:
            image_data_url = download_to_data_url(img)
    if not image_data_url and remote_full:
        img = _pick_first_image_url(remote_full.get("images") or [])
        if img:
            image_data_url = download_to_data_url(img)
    if not image_data_url and USE_UPLOADED_SAMPLE_IMAGE:
        image_data_url = file_to_data_url(SAMPLE_UPLOADED_IMAGE)

    # stash_ids: prefer explicit in source, else remote_full.id
    stash_ids = []
    possible_id = st.get("stash_id") or st.get("stashdb_id") or st.get("id")
    if possible_id:
        stash_ids.append({"stash_id": possible_id, "endpoint": STASHBOX_URL})
    elif remote_full and remote_full.get("id"):
        stash_ids.append({"stash_id": remote_full.get("id"), "endpoint": STASHBOX_URL})

    # favorite if remote says so
    favorite_val = None
    if remote_full and remote_full.get("is_favorite") is True:
        favorite_val = True

    payload = {
        "name": st.get("name"),
        "aliases": st.get("aliases", []),
        "url": chosen_url,
        "details": details_text or None,
        "parent_id": None,
        "image": image_data_url,
        "tag_ids": tag_ids or None,
        "ignore_auto_tag": st.get("ignore_auto_tag", False),
    }
    if stash_ids:
        payload["stash_ids"] = stash_ids
    if favorite_val is not None:
        payload["favorite"] = favorite_val

    # strip None values
    payload = {k: v for k, v in payload.items() if v is not None}
    return payload

def ensure_parent_id(st):
    parent = st.get("parent")
    if not parent:
        return None
    parent_name = parent.get("name") if isinstance(parent, dict) else parent
    if not parent_name:
        return None

    found = find_studio_by_name(parent_name)
    if found:
        pid = found["id"]
        logger.info(f"Parent found locally: {parent_name} (id={pid})")
        return pid

    logger.info(f"Parent missing locally — fetching full details for '{parent_name}' from StashDB")
    full = fetch_full_studio(parent_name, use_stashbox=True)
    if not full:
        logger.warning(f"Remote lookup failed for parent '{parent_name}', creating minimal parent locally")
        minimal = {"name": parent_name}
        created = gql(CREATE_STUDIO, {"input": minimal}, use_stashbox=False)
        if created and created.get("data") and "errors" not in created:
            pid = created["data"]["studioCreate"]["id"]
            logger.success(f"Created minimal parent '{parent_name}' (id={pid})")
            return pid
        logger.error(f"Failed to create minimal parent '{parent_name}': {created}")
        return None

    payload = {
        "name": full.get("name") or parent_name,
        "aliases": full.get("aliases") or [],
    }

    # pick main url (single)
    first_url = _pick_first_url_from_urlobjs(full.get("urls") or [])
    if first_url:
        payload["url"] = first_url

    # add details with all urls
    all_urls = _collect_all_urls_from_urlobjs(full.get("urls") or [])
    if all_urls:
        payload["details"] = "All URLs:\n" + "\n".join(f"- {u}" for u in all_urls)

    # image
    img_url = _pick_first_image_url(full.get("images") or [])
    if img_url:
        payload["image"] = download_to_data_url(img_url)

    # stash_ids from remote
    remote_id = full.get("id")
    if remote_id:
        payload["stash_ids"] = [{"stash_id": remote_id, "endpoint": STASHBOX_URL}]

    # favorite
    if full.get("is_favorite") is True:
        payload["favorite"] = True

    # parent-of-parent one level
    fp_parent = full.get("parent")
    if fp_parent and isinstance(fp_parent, dict):
        gp_name = fp_parent.get("name")
        if gp_name:
            gp_found = find_studio_by_name(gp_name)
            if gp_found:
                payload["parent_id"] = gp_found["id"]
            else:
                logger.info(f"Creating grandparent '{gp_name}'")
                gp_id = ensure_parent_id({"parent": {"name": gp_name}})
                if gp_id:
                    payload["parent_id"] = gp_id

    logger.info(f"Creating parent with full details: {payload.get('name')}")
    created = gql(CREATE_STUDIO, {"input": payload}, use_stashbox=False)
    if created and created.get("data") and "errors" not in created:
        pid = created["data"]["studioCreate"]["id"]
        logger.success(f"Created parent '{payload.get('name')}' (local id={pid})")
        return pid
    logger.error(f"Failed to create parent '{payload.get('name')}': {created}")
    return None

# -----------------------
# Main import loop
# -----------------------
def main():
    if not JSON_FILE.exists():
        logger.error(f"studios_list.json not found at {JSON_FILE}")
        return

    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            studios = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON file: {e}")
        return

    logger.info(f"Loaded {len(studios)} studios from {JSON_FILE}")

    created = []
    updated = []
    failed = []

    for st in studios:
        name = st.get("name")
        if not name:
            continue

        # Resume logic
        first_letter = name.strip()[0:1].upper()
        if first_letter < RESUME_FROM_LETTER.upper():
            continue

        logger.info(f"→ Processing studio: {name}")


        # Attempt to fetch remote full info (non-fatal)
        remote_full = None
        try:
            remote_full = fetch_full_studio(name, use_stashbox=True)
        except Exception:
            remote_full = None

        # find existing local
        existing = find_studio_by_name(name)
        parent_id = ensure_parent_id(st) if st.get("parent") else None

        payload = create_studio_payload(st, remote_full=remote_full)
        if parent_id:
            payload["parent_id"] = parent_id

        # ensure stash_ids contains remote_full id if present
        if remote_full and remote_full.get("id"):
            sid_obj = {"stash_id": remote_full["id"], "endpoint": STASHBOX_URL}
            if "stash_ids" in payload:
                if not any(x.get("endpoint") == sid_obj["endpoint"] and x.get("stash_id") == sid_obj["stash_id"] for x in payload["stash_ids"]):
                    payload.setdefault("stash_ids", []).append(sid_obj)
            else:
                payload["stash_ids"] = [sid_obj]

        if existing:
            studio_id = existing["id"]
            logger.info(f"   Found existing studio {studio_id} → updating")
            payload["id"] = studio_id
            res = gql(UPDATE_STUDIO, {"input": payload}, use_stashbox=False)
            if res and res.get("data") and "errors" not in res:
                logger.success(f"   ✅ Updated: {name}")
                updated.append(name)
            else:
                logger.error(f"   ❌ Update failed: {res}")
                failed.append(name)
        else:
            logger.info("   Not found → creating")
            res = gql(CREATE_STUDIO, {"input": payload}, use_stashbox=False)
            if res and res.get("data") and "errors" not in res:
                logger.success(f"   ✅ Created: {name}")
                created.append(name)
            else:
                logger.error(f"   ❌ Create failed: {res}")
                failed.append(name)

    # Summary
    logger.info("==============================")
    logger.info("IMPORT SUMMARY")
    logger.info("==============================")
    logger.info(f"Created: {len(created)}")
    logger.info(f"Updated: {len(updated)}")
    logger.info(f"Failed:  {len(failed)}")
    if failed:
        logger.info("Failed items:")
        for f in failed:
            logger.info(f" - {f}")

if __name__ == "__main__":
    main()

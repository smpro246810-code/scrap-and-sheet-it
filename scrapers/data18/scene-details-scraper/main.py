"""
Scene details scraper for Data18.

This module fetches a single scene page from Data18, parses detailed
information (duration, tags, movie metadata, performers, and external
redirects) and writes the structured JSON to the scraper's `data/`
directory. It also loads an external age-verification helper dynamically
from the shared `scrapers/setup/age-verification` script when available.
"""

import json
import logging
import re
import time
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
import importlib.util

import requests
from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Project root and dynamic age-verification loader
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)


def load_age_verification():
    """Dynamically load `ensure_age_verification` from the shared
    age-verification script.

    Returns the `ensure_age_verification(driver, logger)` callable if
    available, otherwise returns None.
    """

    if not AGE_VERIFICATION_PATH.exists():
        return None

    spec = importlib.util.spec_from_file_location("age_verification", str(AGE_VERIFICATION_PATH))
    if not spec or not spec.loader:
        return None

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return getattr(module, "ensure_age_verification", None)
    except Exception:
        # Any import-time failure should not crash the scraper; fall back.
        return None


def ensure_age_verification_fallback(driver, logger=None):
    """Fallback no-op used when the shared age verification module is
    unavailable. This keeps scrapers robust in isolated environments.
    """

    if logger:
        logger.info("Age verification module unavailable; skipping check.")


ensure_age_verification = load_age_verification() or ensure_age_verification_fallback

# Base/data paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- LOGGER ----------------
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

# ---------------- DRIVER SETUP ----------------
def create_driver(headless=False):
    """Create and return a configured Chrome WebDriver instance.

    The driver is pre-configured with common options used across
    scrapers (viewport size, user-agent, and anti-detection flags).
    """

    options = Options()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")

    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )
    options.add_argument(f"user-agent={ua}")

    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ---------------- HELPERS ----------------
def is_server_error_page(driver):
    """Heuristically detect server error / anti-bot pages from HTML.

    Returns True if the page contains known server-error or anti-bot
    strings, in which case the scraper should back off.
    """

    html = driver.page_source.lower()
    bad_signals = [
        "http error 500",
        "unable to handle this request",
        "server error",
        "cloudflare",
        "checking your browser before accessing",
        "/cdn-cgi/l/chk_jschl",
    ]
    return any(sig in html for sig in bad_signals)

def resolve_external_link(url, logger=None):
    """Follow redirect from data18.com/g/... to real site."""
    if not url:
        return {
            "original_site_redirect_url": None,
            "original_site_final_url": None
        }

    result = {
        "original_site_redirect_url": url,
        "original_site_final_url": None,
    }

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        response = requests.get(
            url,
            allow_redirects=True,
            timeout=12,
            headers=headers,
        )

        final_url = response.url

        # If still on data18, check for meta-refresh redirect
        if "data18.com" in final_url.lower():
            match = re.search(r'url=(https?://[^\s"\']+)', response.text, re.IGNORECASE)
            if match:
                final_url = match.group(1)

        result["original_site_final_url"] = final_url
        return result

    except requests.exceptions.SSLError as e:
        if logger:
            logger.warning(f"⚠️ SSL error while resolving {url}: {e}")
        result["original_site_final_url"] = url
        return result

    except requests.exceptions.RequestException as e:
        if logger:
            logger.warning(f"⚠️ Could not resolve redirect for {url}: {e}")
        result["original_site_final_url"] = url
        return result

def safe_attr(value: Any) -> str:
    """Converts a BeautifulSoup attribute value into a plain string."""
    if isinstance(value, list):
        value = " ".join(v for v in value if isinstance(v, str))
    return str(value or "").strip()

def safe_lower(value: Any) -> str:
    """Lowercase safely."""
    if isinstance(value, list):
        value = " ".join(v for v in value if isinstance(v, str))
    return str(value or "").lower()

def extract_scene_number(value: str) -> int:
    """Extract the first integer from a string, return 0 if not found."""
    match = re.search(r"(\d+)", value or "")
    return int(match.group(1)) if match else 0

def format_duration(raw_duration: str) -> str:
    """
    Convert various duration formats into 'X min, Y sec' or 'X hr, Y min, Z sec'.
    Examples:
      - "36:13"            ➜ "36 min, 13 sec"
      - "1:36:13"          ➜ "1 hr, 36 min, 13 sec"
      - "1hr, 36 min, 13 sec" ➜ stays as "1 hr, 36 min, 13 sec" (already valid)
    """
    raw_duration = raw_duration.strip()

    # Case: "hh:mm:ss" or "mm:ss"
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", raw_duration):
        parts = raw_duration.split(":")
        if len(parts) == 2:
            m, s = parts
            return f"{int(m)} min, {int(s)} sec"
        elif len(parts) == 3:
            h, m, s = parts
            return f"{int(h)} hr, {int(m)} min, {int(s)} sec"

    # Case: "1hr, 36 min, 13 sec" or similar (already ok)
    if re.search(r"hr|min|sec", raw_duration):
        return raw_duration

    # Default fallback (just return raw text)
    return raw_duration


# ---------------- PARSING ----------------
def parse_scene_details(html, scene_url, logger):
    """Parse the HTML for a single scene page and return a dict.

    The returned dict contains keys such as `duration`, `tags`, movie
    metadata, and any resolved external redirect URLs. The `scene_url`
    parameter is provided for context and logging.
    """

    soup = BeautifulSoup(html, "html.parser")

    # --- Detect if this is a movie scene ---
    movie_div = soup.find("div", style=re.compile("position: relative; margin-bottom: 3px"))
    is_movie = bool(movie_div)

    # --- Initialize result ---
    result = {
        "duration": None,
        "tags": {},
        "original_site_redirect_url": None,
        "original_site_final_url": None,
    }

    if is_movie:
        result["is_movie"] = True
        result["movie_segment"] = None

    # --- Duration & Movie Segment ---
    if is_movie:
        dur_tag = soup.find(lambda tag: tag.name == "p" and "Duration" in tag.get_text())
        if dur_tag:
            bold = dur_tag.find("b")
            if bold:
                raw_duration = bold.get_text(strip=True)
                result["duration"] = format_duration(raw_duration)
            span = dur_tag.find("span", class_="genmed")
            if span:
                match = re.search(r"(\d{2}:\d{2}:\d{2}\s*-\s*\d{2}:\d{2}:\d{2})", span.get_text(strip=True))
                if match:
                    result["movie_segment"] = match.group(1)
    else:
        duration_match = re.search(r'Duration:\s*<b>([\d:]+)</b>', str(soup), re.IGNORECASE)
        if duration_match:
            raw_duration = duration_match.group(1)
            result["duration"] = format_duration(raw_duration)

    # --- Tags ---
    tags_container = None
    for div in soup.find_all("div"):
        if div.get_text(strip=True).startswith("Categories"):
            tags_container = div
            break

    if tags_container:
        current_group = "Categories"
        for elem in tags_container.descendants:
            if isinstance(elem, Tag):
                text = elem.get_text(strip=True)
                if elem.name in ["b", "span"] and text.endswith(":"):
                    current_group = text.replace(":", "")
                    result["tags"].setdefault(current_group, [])
                elif elem.name == "a":
                    tag_name = text.replace("\xa0", " ")
                    result["tags"].setdefault(current_group, []).append(tag_name)

    # --- Original Site Link ---
    moviewrap = soup.find("div", id="moviewrap2")
    if moviewrap:
        a_tag = moviewrap.find("a", href=re.compile(r"^https://www\.data18\.com/g/"))
        if a_tag:
            external_url = a_tag.get("href")
            resolved = resolve_external_link(external_url, logger)
            result.update(resolved)

    # --- Movie Section ---
    if is_movie:
        movie_title = movie_url = cover_front = cover_back = None

        link = movie_div.find("a", href=re.compile(r"/movies/"))

        if isinstance(link, Tag):
            movie_title = safe_attr(link.get("title"))
            # Remove trailing #X (e.g. "#2")
            movie_title = re.sub(r"\s*#\d+\s*$", "", movie_title).strip()
            movie_href = safe_attr(link.get("href"))
            movie_url = urljoin("https://www.data18.com", movie_href)

        else:
            movie_title = ""
            movie_url = ""

        front = movie_div.find("a", {"data-title": re.compile("Front", re.I)})
        back = movie_div.find("a", {"data-title": re.compile("Back", re.I)})
        if front:
            cover_front = front["href"]
        if back:
            cover_back = back["href"]

        # --- Related Scenes & Episodes ---
        related_div = soup.find("div", id="relatedscenes")
        movie_related_scenes = []
        miniseries_episodes = []

        # We'll collect the labels first, then add the "current" items once we know both.
        current_scene_label = None   # e.g., "Scene 1"
        current_episode_label = None # e.g., "Episode 5"

        if related_div:
            # Extract movie scenes under class="moviequick Scrollable"
            moviequick_div = related_div.find("div", class_="moviequick")
            if moviequick_div:
                scene_links = moviequick_div.find_all("a", href=re.compile(r"/scenes/"))
                for link in scene_links:
                    rel_scene = {
                        "url": safe_attr(link.get("href")),
                        "title": safe_attr(link.get("title")),
                        "scene_number": None,
                        "thumbnail": None,
                        "performers": []
                    }

                    num_tag = link.find("b")
                    if num_tag:
                        rel_scene["scene_number"] = num_tag.get_text(strip=True)

                    img_tag = link.find("img")
                    if img_tag and img_tag.get("src"):
                        rel_scene["thumbnail"] = img_tag["src"]

                    performers_div = link.find("div", class_="genmed")
                    if performers_div:
                        performers = [p.strip() for p in performers_div.stripped_strings if p.strip()]
                        rel_scene["performers"] = performers

                    movie_related_scenes.append(rel_scene)

            # Detect the current movie scene div (not in an anchor)
            current_scene_div = related_div.find(
                lambda tag: tag.name == "div"
                and "current scene" in tag.get_text(strip=True).lower()
                and "#fff8f9" in safe_lower(tag.get("style"))
            )
            if current_scene_div:
                match = re.search(r"(Scene\s*\d+)", current_scene_div.get_text(strip=True), re.IGNORECASE)
                if match:
                    current_scene_label = match.group(1)  # e.g., "Scene 1"

            # Extract miniseries episodes under class="relatedminiserie scroll"
            miniseries_div = related_div.find("div", class_="relatedminiserie")
            if miniseries_div:
                episode_links = miniseries_div.find_all("a", href=re.compile(r"/scenes/"))
                for link in episode_links:
                    ep_scene = {
                        "url": safe_attr(link.get("href")),
                        "title": safe_attr(link.get("title")),
                        "episode_number": None,
                        "thumbnail": None,
                        "performers": []
                    }

                    num_tag = link.find("b")
                    if num_tag:
                        ep_scene["episode_number"] = num_tag.get_text(strip=True)

                    img_tag = link.find("img")
                    if img_tag and img_tag.get("src"):
                        ep_scene["thumbnail"] = img_tag["src"]

                    performers_div = link.find("div", class_="genmed")
                    if performers_div:
                        performers = [p.strip() for p in performers_div.stripped_strings if p.strip()]
                        ep_scene["performers"] = performers

                    miniseries_episodes.append(ep_scene)

                # Detect the current episode div (not in an anchor)
                current_ep_div = miniseries_div.find(
                    lambda tag: tag.name == "div"
                    and "current scene" in tag.get_text(strip=True).lower()
                    and "#fff8f9" in safe_lower(tag.get("style"))
                )
                if current_ep_div:
                    match = re.search(r"(Episode\s*\d+)", current_ep_div.get_text(strip=True), re.IGNORECASE)
                    if match:
                        current_episode_label = match.group(1)  # e.g., "Episode 5"

            # Now add the synthetic "current" items, preferring Episode label for the title when available
            if current_scene_label:
                # Title prefers Episode X; falls back to Scene Y if no episode label found
                title_label = current_episode_label or current_scene_label
                movie_related_scenes.append({
                    "title": f"{movie_title}: {title_label}",
                    "scene_number": current_scene_label,   # keep the actual scene label here
                    "is_current_scene": True
                })

            if current_episode_label:
                miniseries_episodes.append({
                    "title": f"{movie_title}: {current_episode_label}",
                    "episode_number": current_episode_label,
                    "is_current_episode": True
                })

            # Sort lists
            movie_related_scenes.sort(key=lambda s: extract_scene_number(s.get("scene_number", "")))
            miniseries_episodes.sort(key=lambda s: extract_scene_number(s.get("episode_number", "")))

        # If no episodes exist, trim down current_movie_scene structure
        if not miniseries_episodes:
            for scene in movie_related_scenes:
                if scene.get("is_current_scene"):
                    # Remove title — keep only scene_number and is_current_scene
                    scene.pop("title", None)

        # Consolidated movie info
        movie_info = {
            "title": movie_title,
            "url": movie_url,
            "cover_front": cover_front,
            "cover_back": cover_back,
            "total_movie_scenes": len(movie_related_scenes),
            "movie_scenes": movie_related_scenes,
        }

        # Add episodes only if present
        if miniseries_episodes:
            movie_info["total_episodes"] = len(miniseries_episodes)
            movie_info["episodes"] = miniseries_episodes


        result["movie"] = movie_info

    return result

# ---------------- SAVE TO JSON ----------------
def save_details_to_json(data, scene_id):
    """Append or write scene details to a per-scene JSON file.

    If an existing file contains a list, this function will append only
    new records (unique by `scene_url`). Returns the path written.
    """

    json_path = DATA_DIR / f"{scene_id}_DETAILS.json"

    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(existing, list):
                existing_urls = {x.get("scene_url") for x in existing}
                new_data = [d for d in data if d.get("scene_url") not in existing_urls]
                existing.extend(new_data)
                data = existing
        except Exception:
            pass

    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"💾 Saved {len(data)} records to {json_path}")
    return json_path

# ---------------- MAIN ----------------
def main():
    scene_id = input("Enter the Data18 scene ID (e.g. 391785 or 1350558-the-brazzers-podcast-episode-5): ").strip()
    if not scene_id:
        print("❌ Invalid input. Please enter a scene ID.")
        return

    TEST_SCENE_URL = f"https://www.data18.com/scenes/{scene_id}"
    logger.info(f"Fetching details for scene ID: {scene_id}")

    driver = create_driver(headless=False)
    try:
        driver.get(TEST_SCENE_URL)
        time.sleep(4)

        ensure_age_verification(driver, logger)
        time.sleep(3)

        if is_server_error_page(driver):
            logger.error("❌ Server error detected.")
            return

        html = driver.page_source


        result = parse_scene_details(html, TEST_SCENE_URL, logger)

        save_details_to_json([result], scene_id)


    except Exception as e:
        logger.error(f"🚨 Error fetching details: {e}", exc_info=True)

    # finally:
    #     driver.quit()

if __name__ == "__main__":
    main()

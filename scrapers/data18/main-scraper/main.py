#!/usr/bin/env python3
"""
unified_scenes_scraper.py

Single-driver unified Data18 scraper (performer -> scene -> movie).
- One Chrome driver for whole session
- Prompts once for headless mode
- Handles age gate (calls project's ensure_age_verification if available; fallback heuristics)
- Pagination + incremental saving
- Fixes missing fields (pair_url, scenes_count, trailer_url placement)
- Extracts scene details and movie details and merges them into final JSON
- Overwrites file at start of run
"""

import json
import random
import re
import sys
import time
import inquirer
import unicodedata
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
    ElementClickInterceptedException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from colorama import Fore, Style, init
import importlib.util

# ---------------- PATHS & BASES ----------------
# BASE_DIR set two levels above this file (project root)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "logs.log"

# Project root and dynamic age-verification loader
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)


def load_age_verification():
    """Attempt to load `ensure_age_verification` dynamically from the
    shared project helper. Returns the callable or None.
    """
    if not AGE_VERIFICATION_PATH.exists():
        return None
    spec = importlib.util.spec_from_file_location(
        "age_verification", str(AGE_VERIFICATION_PATH)
    )
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return getattr(module, "ensure_age_verification", None)
    except Exception:
        return None


# Try to load the project's age gate helper; fall back to None.
ensure_age_verification = load_age_verification()

# ---------------- LOGGING ----------------
init(autoreset=True)


def log(message: str, level: str = "info", print_console: bool = True):
    """Unified colored logger with file append."""
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
        print(f"{color}{timestamp} {tag:<10}{Style.RESET_ALL} {message}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {tag} {message}\n")


class LoggerAdapter:
    """Adapter for functions that expect logger.info / warning / error"""

    def info(self, msg):
        log(msg, "info")

    def success(self, msg):
        log(msg, "success")

    def warning(self, msg):
        log(msg, "warning")

    def error(self, msg):
        log(msg, "error")


# ---------------- WEBDRIVER SETUP ----------------
def make_chrome_options(headless: bool) -> Options:
    """Build Chrome WebDriver options with anti-detection flags.

    Args:
        headless: If True, run in headless mode (invisible browser window).

    Returns:
        Configured Options object ready for Chrome WebDriver.
    """
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    # Anti-detection flags to avoid bot detection
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Randomized User-Agent to vary requests
    ua = (
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{random.randint(120,142)}.0.{random.randint(1000,9999)}.137 Safari/537.36"
    )
    opts.add_argument(f"user-agent={ua}")
    return opts


def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Create and configure a selenium Chrome WebDriver."""
    opts = make_chrome_options(headless)
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=opts
        )
        return driver
    except WebDriverException as e:
        log(f"❌ Failed to start ChromeDriver: {e}", "error")
        raise


# ---------------- UTILITIES ----------------
def slugify(name: str) -> str:
    """Convert a name to a URL-safe slug (lowercase, hyphens only).

    Removes diacritics, replaces non-alphanumeric chars with hyphens, and converts
    to lowercase. Used for building paging URLs and output filenames.

    Args:
        name: Input string (e.g., performer or studio name).

    Returns:
        Slugified string (e.g., 'sunny-leone').
    """
    name = unicodedata.normalize("NFKD", (name or ""))
    name = re.sub(r"[^a-zA-Z0-9]+", "-", name)  # Replace non-alphanumeric with hyphens
    return name.strip("-").lower()  # Remove leading/trailing hyphens and lowercase


def is_server_error_page_html(html: str) -> bool:
    """Detect if the HTML page indicates a server error or anti-bot protection.

    Looks for known error signatures and Cloudflare/WAF indicators that suggest
    the page was blocked or the server is unavailable.

    Args:
        html: Page HTML source to check.

    Returns:
        True if error/protection detected, False otherwise.
    """
    html_low = (html or "").lower()
    bad_signals = [
        "http error 500",
        "unable to handle this request",
        "server error",
        "cloudflare",
        "checking your browser before accessing",
        "attention required!",
        "/cdn-cgi/l/chk_jschl",  # Cloudflare challenge endpoint
    ]
    return any(sig in html_low for sig in bad_signals)


def wait_for_performer_loaded(driver: webdriver.Chrome, timeout: int = 15) -> bool:
    """Wait until performer scene item blocks are present and page isn't an ad/age/captcha page."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='item']"))
        )
        html = driver.page_source.lower()
        if any(term in html for term in ("adults only", "captcha", "please verify")):
            return False
        return True
    except Exception:
        return False


def try_click_age_gate_fallback(driver: webdriver.Chrome, logger: LoggerAdapter):
    """
    Attempt several reasonable clicks to bypass the age gate when ensure_age_verification
    isn't available or didn't work. This tries a few common selectors/buttons.
    """
    logger.info("🔍 Checking for age verification...")
    try:
        # common buttons / phrases used by Data18-style gates
        candidates = [
            "//button[contains(., 'ENTER - data18.com')]",
            "//button[contains(., 'Enter') or contains(., 'ENTER')]",
            "//a[contains(., 'ENTER') or contains(., 'Enter')]",
            "//button[contains(., 'I am 18') or contains(., 'I am 21')]",
            "//input[@type='submit' and (contains(@value,'Enter') or contains(@value,'ENTER'))]",
        ]
        for xpath in candidates:
            try:
                el = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].scrollIntoView(true);", el)
                el.click()
                logger.info(f"✅ Clicked: xpath -> {xpath}")
                time.sleep(1.2)
                return True
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"Age-gate fallback error: {e}")
    logger.info("🔎 No clickable standard age gate found (or it failed).")
    return False


# ---------------- PAGINATION SCRAPER ----------------
def load_pages_incrementally(
    driver: webdriver.Chrome, logger: LoggerAdapter, wait_time: int = 2
):
    """
    Yield batches of raw HTML for new scene blocks per page.
    This respects seen ids and stops when no new scenes are found.
    """
    logger.info("🔄 Starting incremental scene page scraping...")
    seen_ids = set()
    current_page = 1
    while True:
        time.sleep(1.0)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        scene_divs = soup.find_all("div", id=re.compile(r"^item"))
        logger.info(f"🎞️ Page {current_page}: Found {len(scene_divs)} scene blocks.")
        new_batch = []
        for div in scene_divs:
            sid = div.get("id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                new_batch.append(str(div))
        if not new_batch:
            logger.warning("⚠️ No new scenes detected — likely end reached.")
            break
        yield "<html><body>" + "\n".join(new_batch) + "</body></html>", len(
            new_batch
        ), current_page
        # try clicking next
        try:
            next_button = driver.find_element(
                By.XPATH, "//div[contains(@id, 'spagea') and contains(., 'Next')]"
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            driver.execute_script("arguments[0].click();", next_button)
            logger.info(f"➡️ Moving to page {current_page + 1}...")
            current_page += 1
            time.sleep(wait_time)
        except NoSuchElementException:
            logger.success("✅ No more pages found. Finished pagination.")
            break
        except Exception as e:
            logger.warning(f"⚠️ Error during pagination: {e}")
            break


# ---------------- PARSING UTILITIES ----------------
def normalize_label(label: str) -> str:
    return (label or "").replace("\xa0", " ").strip().lower().rstrip(":")


def extract_performers_and_pairings(p_tag: Tag) -> List[Dict[str, Any]]:
    """Extract performer names, URLs and scene count pairings from a <p> tag.

    Parses a <p> tag looking for performer links and optional pairing links
    (e.g., /names/pairings/...) that contain scene counts like '[3]' or '+5'.
    Builds performer dicts with name, url, scenes_count, and pair_url if found.

    Args:
        p_tag: BeautifulSoup Tag containing performer list (<p> element).

    Returns:
        List of performer dicts with keys: name, url, scenes_count (optional), pair_url (optional).
    """
    performers = []
    links = p_tag.find_all("a", href=True)
    i = 0
    while i < len(links):
        link = links[i]
        name = link.get_text(" ", strip=True)
        href = link["href"]
        performer = {"name": name, "url": href}
        # Next link may be a pairings link like /names/pairings/... with "[3]"
        if i + 1 < len(links):
            nxt = links[i + 1]
            if "pairings" in nxt.get("href", ""):
                text = nxt.get_text(strip=True).replace("[", "").replace("]", "")
                count_text = re.sub(r"[^\d+]", "", text)
                try:
                    performer["scenes_count"] = (
                        str(int(count_text.replace("+", "")) + 1) if count_text else "1"
                    )
                except Exception:
                    performer["scenes_count"] = "1"
                performer["pair_url"] = nxt["href"]
                i += 1
        performers.append(performer)
        i += 1
    return performers


def extract_field_from_p(p_tag: Tag) -> Optional[Dict[str, Any]]:
    """Extract Studio / Site / Network / Webserie fields from a <p> tag."""
    links = p_tag.find_all("a", href=True)
    if links:
        field = {"name": links[0].get_text(strip=True), "url": links[0]["href"]}
        # possible second link with [x] scenes
        if len(links) > 1:
            extra = links[1]
            if "[" in extra.get_text(strip=True):
                txt = extra.get_text(strip=True).replace("[", "").replace("]", "")
                count_text = re.sub(r"[^\d+]", "", txt)
                try:
                    field["scenes_count"] = (
                        str(int(count_text.replace("+", "")) + 1) if count_text else "1"
                    )
                except Exception:
                    field["scenes_count"] = "1"
                field["pair_url"] = extra["href"]
        return field
    # fallback: "Studio: Name"
    text = p_tag.get_text(" ", strip=True)
    if ":" in text:
        parts = text.split(":", 1)
        name = parts[1].strip()
        if name:
            return {"name": name, "url": None}
    return None


def parse_scene_blocks(all_html: str) -> List[Dict[str, Any]]:
    """Parse performer scene list HTML and extract individual scene entries.

    Finds all <div id='item...'>  blocks on a performer page and extracts:
    - Scene ID, date, title, URL, thumbnail
    - VR video indicator, trailer link
    - Performers, studio, network, site, webserie (with counts and pairing URLs)

    Args:
        all_html: Full HTML of a performer scene list page.

    Returns:
        List of scene dicts, each with keys: scene_id, date, scene_title, scene_url,
        thumbnail, performers (list), studio (dict), etc.
    """
    soup = BeautifulSoup(all_html, "html.parser")
    items = soup.find_all("div", id=re.compile(r"^item"))
    scenes: List[Dict[str, Any]] = []
    for item in items:
        scene: Dict[str, Any] = {}
        # header: ID and date
        header_div = item.find("div", class_="genmed")
        if isinstance(header_div, Tag):
            bold = header_div.find("b")
            if bold:
                scene_id_raw = bold.get_text(strip=True)
                scene_id = re.sub(r"\D", "", scene_id_raw)
                scene["scene_id"] = int(scene_id) if scene_id.isdigit() else None
                full_text = header_div.get_text(" ", strip=True)
                date_text = full_text.replace(scene_id_raw, "").strip()
                scene["date"] = date_text.lstrip("# ").strip()
        # VR badge detection
        purple_div = item.find(
            "div", style=re.compile(r"background:\s*purple", flags=re.I)
        )
        if purple_div and "vr video" in purple_div.get_text(strip=True).lower():
            scene["is_vr_video"] = True
        # trailer link inside the block (#trailer)
        trailer_tag = item.find("a", href=re.compile(r"#trailer"))
        if trailer_tag:
            scene["trailer_url"] = trailer_tag.get("href", "")
        # title & scene_url
        title_div = item.find(
            "div", style=re.compile(r"background:\s*#959595", flags=re.I)
        )
        if title_div:
            a = title_div.find("a", href=True)
            if a:
                scene["scene_title"] = a.get_text(strip=True)
                scene["scene_url"] = a.get("href", "")
        # thumbnail
        img_tag = item.find("img")
        scene["thumbnail"] = img_tag.get("src") if img_tag else None
        # other fields <p>
        for p in item.find_all("p"):
            label_raw = p.get_text(" ", strip=True).split(":")[0]
            label = normalize_label(label_raw)
            if label.startswith("with"):
                scene["performers"] = extract_performers_and_pairings(p)
            elif any(
                tag in label
                for tag in ["studio", "group", "network", "site", "webserie"]
            ):
                scene[label] = extract_field_from_p(p)
            else:
                # generic fallback
                scene[label] = extract_field_from_p(p)
        scenes.append(scene)
    return scenes


# ---------------- FIX / NORMALIZE FIELDS ----------------
def build_pair_url_for_performer(performer_name: str, main_name: str) -> str:
    return f"https://www.data18.com/names/pairings/{slugify(performer_name)}_{slugify(main_name)}"


def build_pair_url_for_studio(studio_name: str, main_name: str) -> str:
    return f"https://www.data18.com/name/{slugify(main_name)}/studios-{slugify(studio_name)}"


def build_pair_url_for_network(network_name: str, main_name: str) -> str:
    return f"https://www.data18.com/name/{slugify(main_name)}/studios-{slugify(network_name)}"


def build_pair_url_for_site(site_name: str, main_name: str) -> str:
    return (
        f"https://www.data18.com/name/{slugify(main_name)}/studios-{slugify(site_name)}"
    )


def insert_field_in_order(
    target_dict: dict, field: str, value: Any, after: str = None
) -> OrderedDict:
    """
    Insert a key-value pair into an OrderedDict right after a specific key.
    If 'after' not found, appends to the end.
    """
    if not isinstance(target_dict, dict):
        return target_dict
    items = list(target_dict.items())
    result = OrderedDict()
    inserted = False
    for k, v in items:
        result[k] = v
        if k == after:
            result[field] = value
            inserted = True
    if not inserted:
        result[field] = value
    return result


def fix_missing_fields(
    scene_list: List[Dict[str, Any]], main_performer_name: str
) -> List[OrderedDict]:
    """Normalize and complete scene data with missing fields and canonical ordering.

    Ensures each scene has:
    - trailer_url positioned after scene_url
    - scenes_count populated (default '1') before pair_url for all entity fields
    - pair_url generated for performers, studio, network, site, webserie if missing

    Returns OrderedDict entries to preserve field order in final JSON output.

    Args:
        scene_list: Raw scene dicts from parse_scene_blocks().
        main_performer_name: Performer name for building pair URLs.

    Returns:
        List of OrderedDict scene objects with normalized fields in canonical order.
    """
    fixed_scenes: List[OrderedDict] = []
    for scene in scene_list:
        fixed_scene = OrderedDict(scene)
        # 1) trailer_url after scene_url
        if "scene_url" in fixed_scene:
            tr = fixed_scene.pop("trailer_url", None)
            if not tr:
                tr = f"{fixed_scene['scene_url']}#trailer"
            fixed_scene = insert_field_in_order(
                fixed_scene, "trailer_url", tr, after="scene_url"
            )
        # 2) fix performers
        performers = []
        for performer in fixed_scene.get("performers", []):
            pd = OrderedDict(performer)
            if "scenes_count" not in pd or not pd["scenes_count"]:
                pd["scenes_count"] = "1"
            if "pair_url" not in pd or not pd["pair_url"]:
                pair_url = build_pair_url_for_performer(pd["name"], main_performer_name)
                pd = insert_field_in_order(
                    pd, "pair_url", pair_url, after="scenes_count"
                )
            else:
                pair_val = pd.pop("pair_url")
                pd = insert_field_in_order(
                    pd, "pair_url", pair_val, after="scenes_count"
                )
            performers.append(pd)
        if performers:
            fixed_scene["performers"] = performers
        # 3) fix studio/network/site/webserie
        for key, builder in [
            ("studio", build_pair_url_for_studio),
            ("network", build_pair_url_for_network),
            ("site", build_pair_url_for_site),
            ("webserie", build_pair_url_for_site),
        ]:
            fld = fixed_scene.get(key)
            if fld and isinstance(fld, dict):
                fd = OrderedDict(fld)
                if "scenes_count" not in fd or not fd["scenes_count"]:
                    fd["scenes_count"] = "1"
                if "pair_url" not in fd or not fd["pair_url"]:
                    fd = insert_field_in_order(
                        fd,
                        "pair_url",
                        builder(fd["name"], main_performer_name),
                        after="scenes_count",
                    )
                else:
                    pair_val = fd.pop("pair_url")
                    fd = insert_field_in_order(
                        fd, "pair_url", pair_val, after="scenes_count"
                    )
                fixed_scene[key] = fd
        fixed_scenes.append(fixed_scene)
    return fixed_scenes


# ---------------- SCENE DETAILS PARSER ----------------
def safe_attr(value: Any) -> str:
    if isinstance(value, list):
        value = " ".join(v for v in value if isinstance(v, str))
    return str(value or "").strip()


def safe_lower(value: Any) -> str:
    if isinstance(value, list):
        value = " ".join(v for v in value if isinstance(v, str))
    return str(value or "").lower()


def extract_scene_number(value: str) -> int:
    match = re.search(r"(\d+)", value or "")
    return int(match.group(1)) if match else 0


def format_duration(raw_duration: str) -> str:
    raw_duration = (raw_duration or "").strip()
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", raw_duration):
        parts = raw_duration.split(":")
        if len(parts) == 2:
            m, s = parts
            return f"{int(m)} min, {int(s)} sec"
        elif len(parts) == 3:
            h, m, s = parts
            return f"{int(h)} hr, {int(m)} min, {int(s)} sec"
    if re.search(r"hr|min|sec", raw_duration):
        return raw_duration
    return raw_duration


def resolve_external_link(
    url: str, logger: LoggerAdapter = None
) -> Dict[str, Optional[str]]:
    """Follow redirect and return original + final target (handle meta-refresh)."""
    if not url:
        return {"original_site_redirect_url": None, "original_site_final_url": None}
    res = {"original_site_redirect_url": url, "original_site_final_url": None}
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        r = requests.get(url, allow_redirects=True, timeout=12, headers=headers)
        final = r.url
        if "data18.com" in final.lower():
            m = re.search(r'url=(https?://[^\s"\']+)', r.text, re.IGNORECASE)
            if m:
                final = m.group(1)
        res["original_site_final_url"] = final
    except requests.exceptions.SSLError as e:
        if logger:
            logger.warning(f"SSL error resolving {url}: {e}")
        res["original_site_final_url"] = url
    except requests.exceptions.RequestException as e:
        if logger:
            logger.warning(f"Request error resolving {url}: {e}")
        res["original_site_final_url"] = url
    return res


def parse_scene_details_from_html(
    html: str, scene_url: str, logger: LoggerAdapter
) -> Dict[str, Any]:
    """Parse a single scene detail page and extract rich metadata.

    Extracts from scene page HTML:
    - Duration (formatted as 'X min Y sec' or 'X hr Y min Z sec')
    - Tags grouped by category (Categories, Acts, Body types, etc.)
    - Original site redirect (if linked in Data18)
    - Movie info (if scene is part of a movie):
      * Movie title, URL, cover art (front/back)
      * Related scenes and episode list (if miniseries)
      * Performers for each related scene/episode

    Args:
        html: Scene detail page HTML source.
        scene_url: Scene URL (for context and logging).
        logger: Logger instance for warnings.

    Returns:
        Dict with keys: duration, tags, original_site_redirect_url,
        original_site_final_url, is_movie (bool), movie (dict if movie), etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, Any] = {
        "duration": None,
        "tags": {},
        "original_site_redirect_url": None,
        "original_site_final_url": None,
    }

    # Detect movie block
    movie_div = soup.find(
        "div", style=re.compile("position: relative; margin-bottom: 3px")
    )
    is_movie = bool(movie_div)
    if is_movie:
        result["is_movie"] = True
        result["movie_segment"] = None

    # Duration
    if is_movie:
        dur_tag = soup.find(
            lambda tag: tag.name == "p" and "Duration" in tag.get_text()
        )
        if dur_tag:
            bold = dur_tag.find("b")
            if bold:
                result["duration"] = format_duration(bold.get_text(strip=True))
            span = dur_tag.find("span", class_="genmed")
            if span:
                match = re.search(
                    r"(\d{2}:\d{2}:\d{2}\s*-\s*\d{2}:\d{2}:\d{2})",
                    span.get_text(strip=True),
                )
                if match:
                    result["movie_segment"] = match.group(1)
    else:
        duration_match = re.search(
            r"Duration:\s*<b>([\d:]+)</b>", str(soup), re.IGNORECASE
        )
        if duration_match:
            result["duration"] = format_duration(duration_match.group(1))

    # Tags/categories
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

    # Original site redirect link
    moviewrap = soup.find("div", id="moviewrap2")
    if moviewrap:
        a_tag = moviewrap.find("a", href=re.compile(r"^https://www\.data18\.com/g/"))
        if a_tag:
            external_url = a_tag.get("href")
            resolved = resolve_external_link(external_url, logger)
            result.update(resolved)

    # Movie block details (title, url, covers, related scenes, episodes)
    if is_movie:
        movie_title = movie_url = cover_front = cover_back = None
        link = movie_div.find("a", href=re.compile(r"/movies/"))
        if isinstance(link, Tag):
            movie_title = safe_attr(link.get("title"))
            movie_title = re.sub(r"\s*#\d+\s*$", "", movie_title).strip()
            movie_href = safe_attr(link.get("href"))
            movie_url = urljoin("https://www.data18.com", movie_href)
        front = movie_div.find("a", {"data-title": re.compile("Front", re.I)})
        back = movie_div.find("a", {"data-title": re.compile("Back", re.I)})
        if front:
            cover_front = front.get("href")
        if back:
            cover_back = back.get("href")

        related_div = soup.find("div", id="relatedscenes")
        movie_related_scenes = []
        miniseries_episodes = []
        current_scene_label = None
        current_episode_label = None

        if related_div:
            moviequick_div = related_div.find("div", class_="moviequick")
            if moviequick_div:
                scene_links = moviequick_div.find_all("a", href=re.compile(r"/scenes/"))
                for link in scene_links:
                    rel_scene = {
                        "url": safe_attr(link.get("href")),
                        "title": safe_attr(link.get("title")),
                        "scene_number": None,
                        "thumbnail": None,
                        "performers": [],
                    }
                    num_tag = link.find("b")
                    if num_tag:
                        rel_scene["scene_number"] = num_tag.get_text(strip=True)
                    img_tag = link.find("img")
                    if img_tag and img_tag.get("src"):
                        rel_scene["thumbnail"] = img_tag["src"]
                    performers_div = link.find("div", class_="genmed")
                    if performers_div:
                        performers = [
                            p.strip()
                            for p in performers_div.stripped_strings
                            if p.strip()
                        ]
                        rel_scene["performers"] = performers
                    movie_related_scenes.append(rel_scene)

            current_scene_div = related_div.find(
                lambda tag: tag.name == "div"
                and "current scene" in tag.get_text(strip=True).lower()
                and "#fff8f9" in safe_lower(tag.get("style"))
            )
            if current_scene_div:
                match = re.search(
                    r"(Scene\s*\d+)",
                    current_scene_div.get_text(strip=True),
                    re.IGNORECASE,
                )
                if match:
                    current_scene_label = match.group(1)

            miniseries_div = related_div.find("div", class_="relatedminiserie")
            if miniseries_div:
                episode_links = miniseries_div.find_all(
                    "a", href=re.compile(r"/scenes/")
                )
                for link in episode_links:
                    ep_scene = {
                        "url": safe_attr(link.get("href")),
                        "title": safe_attr(link.get("title")),
                        "episode_number": None,
                        "thumbnail": None,
                        "performers": [],
                    }
                    num_tag = link.find("b")
                    if num_tag:
                        ep_scene["episode_number"] = num_tag.get_text(strip=True)
                    img_tag = link.find("img")
                    if img_tag and img_tag.get("src"):
                        ep_scene["thumbnail"] = img_tag["src"]
                    performers_div = link.find("div", class_="genmed")
                    if performers_div:
                        performers = [
                            p.strip()
                            for p in performers_div.stripped_strings
                            if p.strip()
                        ]
                        ep_scene["performers"] = performers
                    miniseries_episodes.append(ep_scene)

                current_ep_div = miniseries_div.find(
                    lambda tag: tag.name == "div"
                    and "current scene" in tag.get_text(strip=True).lower()
                    and "#fff8f9" in safe_lower(tag.get("style"))
                )
                if current_ep_div:
                    match = re.search(
                        r"(Episode\s*\d+)",
                        current_ep_div.get_text(strip=True),
                        re.IGNORECASE,
                    )
                    if match:
                        current_episode_label = match.group(1)

            if current_scene_label:
                title_label = current_episode_label or current_scene_label
                movie_related_scenes.append(
                    {
                        "title": f"{movie_title}: {title_label}",
                        "scene_number": current_scene_label,
                        "is_current_scene": True,
                    }
                )
            if current_episode_label:
                miniseries_episodes.append(
                    {
                        "title": f"{movie_title}: {current_episode_label}",
                        "episode_number": current_episode_label,
                        "is_current_episode": True,
                    }
                )

            movie_related_scenes.sort(
                key=lambda s: extract_scene_number(s.get("scene_number", ""))
            )
            miniseries_episodes.sort(
                key=lambda s: extract_scene_number(s.get("episode_number", ""))
            )

        if not miniseries_episodes:
            for s in movie_related_scenes:
                if s.get("is_current_scene"):
                    s.pop("title", None)

        movie_info = {
            "title": movie_title,
            "url": movie_url,
            "cover_front": cover_front,
            "cover_back": cover_back,
            "total_movie_scenes": len(movie_related_scenes),
            "movie_scenes": movie_related_scenes,
        }
        if miniseries_episodes:
            movie_info["total_episodes"] = len(miniseries_episodes)
            movie_info["episodes"] = miniseries_episodes
        result["movie"] = movie_info

    return result


# ---------------- MOVIE PAGE PARSING (separate enrichment) ----------------
def parse_movie_page_from_driver(
    driver: webdriver.Chrome, movie_url: str, logger: LoggerAdapter
) -> Dict[str, Any]:
    """Navigate to a movie page and extract metadata (release date, length, director, tags).

    Uses the provided driver to visit a movie detail page and parses:
    - Release date
    - Movie length (duration)
    - Director name(s)
    - Tags/categories grouped by type

    This enriches movie metadata for scenes that are part of movies.

    Args:
        driver: Active Selenium WebDriver instance.
        movie_url: URL of the movie detail page.
        logger: Logger instance for errors/warnings.

    Returns:
        Dict with keys: release_date, movie_length, director, tags (grouped by category).
    """
    try:
        driver.get(movie_url)
        time.sleep(2)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        result = {
            "release_date": None,
            "movie_length": None,
            "director": None,
            "tags": {},
        }
        # release date span
        rel_span = soup.find(
            "span", class_="gen11", string=re.compile(r"Release date:", re.I)
        )
        if rel_span:
            txt = rel_span.get_text(strip=True)
            result["release_date"] = txt.replace("Release date:", "").strip()
        # length and director and tags
        for p in soup.find_all("p"):
            text = p.get_text(" ", strip=True)
            if "Length" in text and not result["movie_length"]:
                # may be in <b>Length</b> next sibling
                b = p.find("b")
                if b:
                    # next sibling text
                    nxt = b.next_sibling
                    if nxt:
                        result["movie_length"] = str(nxt).strip()
                if not result["movie_length"]:
                    # fallback: full p text minus "Length:"
                    result["movie_length"] = text.replace("Length:", "").strip()
            if "Director" in text and not result["director"]:
                a = p.find("a")
                if a:
                    result["director"] = a.get_text(strip=True)
            if text.startswith("Categories") or text.startswith("Genre"):
                current_group = "Categories"
                result["tags"][current_group] = []
                for elem in p.descendants:
                    if isinstance(elem, Tag):
                        et = elem.get_text(strip=True)
                        if elem.name in ["b", "span"] and et.endswith(":"):
                            current_group = et.replace(":", "")
                            result["tags"].setdefault(current_group, [])
                        elif elem.name == "a":
                            tag_name = et.replace("\xa0", " ")
                            result["tags"].setdefault(current_group, []).append(
                                tag_name
                            )
        return result
    except Exception as e:
        logger.warning(f"Failed to parse movie page {movie_url}: {e}")
        return {
            "release_date": None,
            "movie_length": None,
            "director": None,
            "tags": {},
        }


def reorder_movie_fields(movie: Dict[str, Any]) -> OrderedDict:
    """Reorder movie object fields for consistent JSON output format.

    Organizes movie metadata extracted from Phase 2B enrichment in logical order:
    - Identity: title, release_date, movie_length, director
    - URLs: url, cover_front, cover_back
    - Content: total_movie_scenes, movie_scenes
    - Classification: tags
    - Extra: any additional fields not in standard order

    Called recursively from reorder_details_fields() as innermost nested object.

    Args:
        movie: Dict with movie metadata (from Phase 2B enrichment).

    Returns:
        OrderedDict with movie fields in canonical order.
    """
    if not isinstance(movie, dict):
        return movie
    order = [
        "title",
        "release_date",
        "movie_length",
        "director",
        "url",
        "cover_front",
        "cover_back",
        "total_movie_scenes",
        "movie_scenes",
        "tags",
    ]
    ordered = OrderedDict()
    for key in order:
        if key in movie:
            ordered[key] = movie[key]
    # append any extra keys not in order
    for k, v in movie.items():
        if k not in ordered:
            ordered[k] = v
    return ordered


def reorder_details_fields(details: Dict[str, Any]) -> OrderedDict:
    """Reorder details object fields for consistent JSON output format.

    Organizes scene detail metadata in logical order:
    - Content: duration, tags
    - URLs: original_site_redirect_url, original_site_final_url
    - Movie association: is_movie, movie_segment, movie (nested)
    - Extra: any additional fields not in standard order

    Called recursively from reorder_scene_fields() to maintain consistent
    nested field ordering throughout the output JSON.

    Args:
        details: Dict with scene detail metadata (may include movie nested object).

    Returns:
        OrderedDict with details fields in canonical order. Movie nested object reordered recursively.
    """
    if not isinstance(details, dict):
        return details
    order = [
        "duration",
        "tags",
        "original_site_redirect_url",
        "original_site_final_url",
        "is_movie",
        "movie_segment",
        "movie",
    ]
    ordered = OrderedDict()
    for key in order:
        if key == "movie" and "movie" in details:
            ordered[key] = reorder_movie_fields(details["movie"])
        elif key in details:
            ordered[key] = details[key]
    for k, v in details.items():
        if k not in ordered:
            ordered[k] = v
    return ordered


def reorder_scene_fields(scene: Dict[str, Any]) -> OrderedDict:
    """Reorder top-level scene fields for consistent JSON output format.

    Organizes scene metadata in a logical order:
    - Identity: scene_id, date, scene_title
    - URLs: scene_url, trailer_url, thumbnail
    - Credits: performers, group, network, studio, site, webserie
    - Enrichment: details (nested reordering)
    - Extra: any additional fields not in standard order

    Maintains deterministic field ordering for human-readable JSON files.

    Args:
        scene: Dict with scene metadata (may have extra keys).

    Returns:
        OrderedDict with fields in canonical order. Details nested object reordered recursively.
    """
    if not isinstance(scene, dict):
        return scene
    order = [
        "scene_id",
        "date",
        "scene_title",
        "scene_url",
        "trailer_url",
        "thumbnail",
        "performers",
        "group",
        "network",
        "studio",
        "site",
        "webserie",
        "details",
    ]
    ordered = OrderedDict()
    for key in order:
        if key == "details" and "details" in scene:
            ordered[key] = reorder_details_fields(scene["details"])
        elif key in scene:
            ordered[key] = scene[key]
    for k, v in scene.items():
        if k not in ordered:
            ordered[k] = v
    return ordered


# ===== SAVE UTILITIES =====
def save_json_atomic(path: Path, data: List[Dict[str, Any]]) -> None:
    """Save JSON data atomically using temp file to prevent corruption on interrupt.

    Writes to a temporary .tmp file first, then atomically replaces the target file.
    This prevents data loss if the script crashes or is interrupted mid-write.

    Args:
        path: Target file path for the JSON output.
        data: List of scene/movie dictionaries to serialize.
    """
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def output_file_for_performer(performer_name: str) -> Path:
    """Generate output file path for a performer's scraped scene data.

    Creates the data directory if it doesn't exist and returns a path
    with the performer's name slugified (URL-safe).

    Args:
        performer_name: Display name of the performer (e.g., "Jane Doe").

    Returns:
        Path object pointing to <scraper_dir>/data/<performer_slug>.json
    """
    performer_slug = slugify(performer_name)
    out_dir = BASE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{performer_slug}.json"


# ---------------- MAIN RUNNER (single driver) ----------------
def run_unified_one_driver(performer_name: str, headless: bool):
    """Unified multi-phase scraper orchestrator for a single performer.

    Executes a comprehensive three-phase scraping workflow:

    PHASE 1 (Pagination & Scene List Extraction):
        - Navigates to performer page and loads scene listing pages in batches
        - Extracts scene metadata: ID, date, title, URL, performers, studio, scene count
        - Implements deep scrolling to trigger pagination
        - Saves incremental batches to output file during processing

    PHASE 2 (Scene Detail Extraction):
        - Navigates to each scene's detail page
        - Extracts expanded metadata: duration, tags, external redirects, movie association
        - Handles Cloudflare challenges, WAF blocks, and error pages with graceful fallback
        - Incremental saving to prevent data loss on interruption

    PHASE 2B (Movie Enrichment - Conditional):
        - For scenes associated with movies, navigates to movie detail pages
        - Extracts movie metadata: release date, movie length, director, tags/categories
        - Enriches scene record with full movie information
        - Skipped if no movie association present

    Uses a single driver instance throughout to maintain session state and performance.
    Implements atomic file writing (tmp → rename) to prevent data corruption on interrupt.

    Args:
        performer_name: Name of the performer to scrape (must match URL format).
        headless: Boolean flag to run Chrome in headless mode (no GUI).
    """
    logger_adapter = LoggerAdapter()
    out_path = output_file_for_performer(performer_name)

    # ensure overwrite behavior
    if out_path.exists():
        try:
            out_path.unlink()
            log(f"🧹 Cleared old file: {out_path}", "info")
        except Exception as e:
            log(f"⚠️ Could not remove old file: {e}", "warning")

    driver = create_driver(headless=headless)
    try:
        performer_url = f"https://www.data18.com/name/{performer_name.strip().lower().replace(' ', '-')}"
        log(f"🔗 Loading performer URL: {performer_url}", "info")
        driver.get(performer_url)
        time.sleep(2)

        # first try project's age gate handler if present
        if ensure_age_verification:
            try:
                ensure_age_verification(driver, logger_adapter)
                time.sleep(1)
            except Exception as e:
                log(f"⚠️ ensure_age_verification raised: {e}", "warning")

        # fallback attempts if page blocked or not loaded
        if not wait_for_performer_loaded(driver, timeout=6):
            log(
                "⚠️ Performer page didn't load normally — attempting fallback age gate clicks",
                "warning",
            )
            try_click_age_gate_fallback(driver, logger_adapter)
            time.sleep(1.5)

        # final check for server/protection
        if is_server_error_page_html(driver.page_source):
            log(
                "❌ Server error / protection detected on performer page. Aborting.",
                "error",
            )
            driver.quit()
            return

        # ===== PHASE 1: PAGINATION & SCENE LIST EXTRACTION =====
        # Incrementally load performer pages (with pagination), parse scene blocks,
        # extract initial metadata (ID, date, title, URL, performers, studio), and
        # save batch-by-batch to output file to ensure incremental progress.
        all_scenes: List[Dict[str, Any]] = []
        for page_html, count, page_num in load_pages_incrementally(
            driver, logger_adapter
        ):
            log(f"🧠 Parsing page {page_num} with {count} scene blocks...", "info")
            parsed = parse_scene_blocks(page_html)
            fixed = fix_missing_fields(parsed, performer_name)
            # append parsed scenes
            all_scenes.extend([dict(s) for s in fixed])  # dict from OrderedDict
            # save intermediate state
            save_json_atomic(out_path, all_scenes)
            log(
                f"💾 Page {page_num}: Saved {len(parsed)} scenes (Total so far: {len(all_scenes)})",
                "success",
            )

        # If no scenes found, warn and finish
        if not all_scenes:
            log(
                "⚠️ No scenes were scraped. Check if performer exists or age gate blocked content.",
                "warning",
            )
            save_json_atomic(out_path, all_scenes)
            return

        # ===== PHASE 2 & 2B: SCENE DETAILS + MOVIE ENRICHMENT (same driver) =====
        # Navigate to each scene's detail page to extract:
        # - Duration (duration)
        # - Tags/categories (tags)
        # - External redirect URLs (redirects)
        # - Movie association (if scene is part of a movie)
        # PHASE 2B (conditional): For scenes with movie association, also scrape
        # - Release date (movie_release_date)
        # - Movie length (movie_length)
        # - Director (movie_director)
        # - Movie tags (movie_tags)
        # Handles Cloudflare challenges, WAF blocks, and error pages with graceful fallback.
        # Saves atomically after each scene to prevent data loss on interrupt.
        merged: List[Dict[str, Any]] = []
        total = len(all_scenes)
        log(
            f"🔁 Starting scene-details scraping for {total} scenes (single driver)...",
            "info",
        )

        for idx, scene in enumerate(all_scenes, start=1):
            scene_url = scene.get("scene_url")
            if not scene_url:
                log(f"⚠️ Scene missing URL at index {idx}. Skipping.", "warning")
                merged.append(scene)
                save_json_atomic(out_path, merged)
                continue

            log(f"🔎 ({idx}/{total}) Loading scene: {scene_url}", "info")
            try:
                driver.get(scene_url)
                time.sleep(2.2)

                # Age gate may reappear on scene pages
                if ensure_age_verification:
                    try:
                        ensure_age_verification(driver, logger_adapter)
                        time.sleep(0.6)
                    except Exception as e:
                        log(
                            f"⚠️ ensure_age_verification raised on scene page: {e}",
                            "warning",
                        )

                # fallback click on scene-level gate if necessary
                if (
                    "adults only" in driver.page_source.lower()
                    or "captcha" in driver.page_source.lower()
                ):
                    try_click_age_gate_fallback(driver, logger_adapter)
                    time.sleep(1.0)

                page_src = driver.page_source
                if is_server_error_page_html(page_src):
                    log(
                        "❌ Server/protection detected on scene page. Aborting scene scraping.",
                        "error",
                    )
                    break

                details = parse_scene_details_from_html(
                    page_src, scene_url, logger_adapter
                )

                # If movie found, enrich movie details by visiting the movie page
                if details.get("is_movie") and details.get("movie", {}).get("url"):
                    movie_url = details["movie"]["url"]
                    log(
                        f"🎬 ({idx}/{total}) Scene is part of movie. Enriching movie: {movie_url}",
                        "info",
                    )
                    movie_meta = parse_movie_page_from_driver(
                        driver, movie_url, logger_adapter
                    )
                    # merge movie_meta into details["movie"]
                    details["movie"].update(movie_meta)

                scene_with_details = dict(scene)
                scene_with_details["details"] = details

                # ensure canonical order for all levels
                ordered_scene = reorder_scene_fields(scene_with_details)

                merged.append(ordered_scene)
                save_json_atomic(out_path, merged)

                log(
                    f"✅ ({idx}/{total}) Merged and saved. Total merged: {len(merged)}",
                    "success",
                )

            except Exception as e:
                log(f"🚨 Error processing scene {scene_url}: {e}", "error")
                merged.append(scene)  # keep original entry if details failed
                save_json_atomic(out_path, merged)
                time.sleep(1.2)
                continue

            # polite delay
            time.sleep(1.0)

        log(f"🎉 Completed scraping. Final file: {out_path}", "success")

    except Exception as e:
        log(f"🚨 Fatal error in run: {e}", "error")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        log("👋 Browser closed. Session ended.", "info")


# ---------------- CLI ENTRY ----------------
if __name__ == "__main__":
    """CLI entry point for the unified Data18 scraper.

    Orchestrates a three-phase scraping workflow with user interaction:
    1. VPN verification (required to access Data18)
    2. Performer name input (determines what scenes to scrape)
    3. Headless mode selection (controls browser visibility)
    4. Initiates run_unified_one_driver() with chosen parameters

    The scraper will:
    - Load performer page and extract scene listings (Phase 1)
    - Navigate to each scene for detailed metadata (Phase 2)
    - For movie scenes, enrich with movie metadata (Phase 2B)
    - Save output to data/<performer_slug>.json with atomic writes
    """
    print()

    # ===== VPN VERIFICATION FIRST =====
    # Data18 content access requires VPN connection to comply with regional restrictions.
    print("🔐 VPN Verification Required\n")
    vpn_question = [
        inquirer.List(
            "vpn",
            message="Are you connected to a VPN?",
            choices=["🔰 Yes, I am connected", "⛔ No, exit scraper"],
        )
    ]

    vpn_answer = inquirer.prompt(vpn_question)["vpn"]

    if vpn_answer.startswith("⛔"):
        print("\n⛔ VPN not connected. Exiting.\n")
        sys.exit(1)

    print("\n🔰 VPN confirmed. Proceeding...\n")

    # ===== PERFORMER NAME INPUT =====
    # Determines which performer's scenes will be scraped in Phases 1-2B.
    performer_name = input("Enter performer name (e.g. 'sunny leone'): ").strip()
    if not performer_name:
        print("No performer name entered. Exiting.")
        sys.exit(1)

    # ===== HEADLESS MODE SELECTION =====
    # Headless: runs Chrome without GUI (faster, quieter, but harder to debug)
    # Non-headless: shows browser (slower, but lets you see page loads and interactions)
    headless_question = [
        inquirer.List(
            "headless",
            message="Run scraper in headless mode?",
            choices=[
                "No (Show Browser)",
                "Yes (Headless)",
            ],
        )
    ]

    headless_answer = inquirer.prompt(headless_question)["headless"]
    headless = headless_answer.startswith("Yes")

    # ===== START UNIFIED SCRAPER =====
    # Initiates the three-phase workflow with a single driver instance.
    run_unified_one_driver(performer_name, headless=headless)

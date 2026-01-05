"""
Data18 Movie Details Scraper
============================

• Scrapes movie-level metadata from Data18
• Handles age verification dynamically
• Extracts release date, length, director, and tags

Output:
  <movie_title>_DETAILS.json
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
import logging
import re
import sys
import time
import importlib.util
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ============================================================
# THIRD-PARTY LIBS
# ============================================================

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# PROJECT PATHS & CONSTANTS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)

DATA18_BASE = "https://www.data18.com"

# ============================================================
# LOGGING
# ============================================================


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = setup_logger()

# ============================================================
# AGE VERIFICATION (SAFE DYNAMIC LOAD)
# ============================================================


def load_age_verification():
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


def ensure_age_verification_fallback(driver, logger=None):
    if logger:
        logger.info("Age verification module unavailable; skipping check.")


ensure_age_verification = load_age_verification() or ensure_age_verification_fallback

# ============================================================
# SELENIUM DRIVER
# ============================================================


def create_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )

    try:
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )
    except WebDriverException as e:
        logger.error(f"🚨 WebDriver initialization failed: {e}")
        raise


# ============================================================
# SAFE HELPERS (LOGIC UNCHANGED)
# ============================================================


def safe_get_text(tag: Optional[Tag]) -> str:
    if tag:
        text = getattr(tag, "get_text", lambda **_: "")(strip=True)
        return text if isinstance(text, str) else ""
    return ""


def safe_next_sibling_text(tag: Optional[Tag]) -> str:
    sibling: Optional[Union[Tag, NavigableString, str]] = getattr(
        tag, "next_sibling", None
    )

    if isinstance(sibling, (NavigableString, str)):
        return str(sibling).strip()

    if isinstance(sibling, Tag):
        return sibling.get_text(strip=True)

    return ""


def is_server_error_page(driver: webdriver.Chrome) -> bool:
    html = driver.page_source.lower()
    bad_signals = [
        "http error 500",
        "unable to handle this request",
        "server error",
        "cloudflare",
        "checking your browser",
        "/cdn-cgi/",
    ]
    return any(sig in html for sig in bad_signals)


# ============================================================
# CORE PARSER (ALL SCRAPING LOGIC PRESERVED)
# ============================================================


def parse_movie_page(html: str) -> Dict[str, Any]:
    """
    Parses a Data18 movie page.

    ⚠️ IMPORTANT:
    This function preserves all original scraping logic.
    No data extraction behavior has been altered.
    """
    soup = BeautifulSoup(html, "html.parser")

    result: Dict[str, Any] = {
        "release_date": None,
        "movie_length": None,
        "director": None,
        "tags": {},
    }

    # ---------------- Release Date ----------------
    release_span = soup.find(
        "span", class_="gen11", string=re.compile(r"Release date:", re.I)
    )
    if release_span:
        text = safe_get_text(release_span)
        result["release_date"] = text.replace("Release date:", "").strip()

    # ---------------- Movie Length ----------------
    for p in soup.find_all("p"):
        b_tag = p.find("b")
        if b_tag and "Length" in safe_get_text(b_tag):
            raw = safe_next_sibling_text(b_tag)
            raw = re.sub(r"\[.*?\]", "", raw).strip()
            result["movie_length"] = raw or None
            break

    # ---------------- Director ----------------
    director_tag = soup.find("b", string=re.compile(r"Director:", re.I))
    if director_tag:
        link = director_tag.find_next("a")
        result["director"] = safe_get_text(link)

    # ---------------- Tags (Grouped) ----------------
    for p in soup.find_all("p"):
        text = safe_get_text(p)
        if text.startswith("Categories:") or text.startswith("Genre:"):
            current_group = "Categories"
            result["tags"].setdefault(current_group, [])

            for elem in p.descendants:
                if isinstance(elem, Tag):
                    elem_text = safe_get_text(elem)
                    if elem.name in ["b", "span"] and elem_text.endswith(":"):
                        current_group = elem_text.replace(":", "")
                        result["tags"].setdefault(current_group, [])
                    elif elem.name == "a":
                        result["tags"][current_group].append(
                            elem_text.replace("\xa0", " ")
                        )

    return result


# ============================================================
# OUTPUT
# ============================================================


def save_movie_info(data: Dict[str, Any], movie_title: str) -> Path:
    safe_title = movie_title.lower().replace(" ", "_")
    path = DATA_DIR / f"{safe_title}_DETAILS.json"

    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info(f"💾 Saved movie info → {path}")
    return path


# ============================================================
# ENTRY POINT
# ============================================================


def main() -> None:
    MOVIE_URL = "https://www.data18.com/movies/1257888-curvy-girls-10"
    MOVIE_TITLE = "Anal Cougar Country"

    driver = create_driver(headless=False)
    try:
        logger.info(f"🌐 Fetching movie page: {MOVIE_URL}")
        driver.get(MOVIE_URL)
        time.sleep(4)

        ensure_age_verification(driver, logger)
        time.sleep(2)

        if is_server_error_page(driver):
            logger.error("❌ Server error detected.")
            return

        movie_info = parse_movie_page(driver.page_source)
        save_movie_info(movie_info, MOVIE_TITLE)

    except Exception as e:
        logger.error(f"🚨 Error fetching movie info: {e}", exc_info=True)
    finally:
        driver.quit()
        logger.info("👋 Browser closed.")


if __name__ == "__main__":
    main()

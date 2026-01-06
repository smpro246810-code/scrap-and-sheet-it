"""
WatchPorn Pornstar Scenes Scraper
--------------------------------

• Uses shared Selenium driver factory
• Unified age-verification handling
• Pagination-safe AJAX scraping
• Lazy-loaded thumbnail resolution
• CLI pornstar selector
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import sys
import json
import time
from pathlib import Path
from typing import List, Dict
import importlib.util


# ============================================================
# THIRD-PARTY
# ============================================================

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ============================================================
# PROJECT SETUP
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

JSON_SOURCE_DIR = PROJECT_ROOT / "scrapers" / "data18" / "main-scraper" / "data"

if not JSON_SOURCE_DIR.exists():
    raise RuntimeError(f"JSON source folder not found: {JSON_SOURCE_DIR}")

# ============================================================
# SHARED HELPERS (REUSED)
# ============================================================
# -------- AGE VERIFICATION --------
AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)


def load_age_verification():
    if not AGE_VERIFICATION_PATH.exists():
        return None

    spec = importlib.util.spec_from_file_location(
        "age_verification", str(AGE_VERIFICATION_PATH)
    )
    if not spec or not spec.loader:
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return getattr(module, "ensure_age_verification", None)


ensure_age_verification = load_age_verification() or (
    lambda driver, logger=None: logger and log("Age verification skipped", level="info")
)

# -------- DRIVER SETUP --------
DRIVER_SETUP_PATH = PROJECT_ROOT / "scrapers" / "setup" / "driver-setup" / "main.py"


def load_driver_setup():
    if not DRIVER_SETUP_PATH.exists():
        raise RuntimeError("driver-setup helper not found")

    spec = importlib.util.spec_from_file_location(
        "driver_setup", str(DRIVER_SETUP_PATH)
    )
    if not spec or not spec.loader:
        raise RuntimeError("Failed to load driver-setup spec")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


_driver_setup = load_driver_setup()
create_driver = _driver_setup.create_driver


# -------- LOGGER SETUP --------
CUSTOM_LOGGER_PATH = PROJECT_ROOT / "scrapers" / "setup" / "custom-logger" / "main.py"


def load_custom_logger():
    if not CUSTOM_LOGGER_PATH.exists():
        raise RuntimeError("custom-logger helper not found")

    spec = importlib.util.spec_from_file_location(
        "custom_logger", str(CUSTOM_LOGGER_PATH)
    )
    if not spec or not spec.loader:
        raise RuntimeError("Failed to load custom-logger spec")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


_logger_utils = load_custom_logger()

log = _logger_utils.log
console_log = _logger_utils.console_log

# 🔥 One-line adapter creation
logger = _logger_utils.CustomLoggerAdapter(log)


# ============================================================
# CLI SELECTION
# ============================================================


def select_pornstar_from_data_folder(path: Path) -> str:
    files = sorted(f for f in path.iterdir() if f.suffix == ".json")

    if not files:
        raise RuntimeError("No pornstar JSON files found.")

    log("📂 Available pornstars:", level="info")
    for i, f in enumerate(files, start=1):
        log(f"{i:2d}. {f.stem.replace('-', ' ').title()}", level="info")

    while True:
        try:
            choice = int(input("Select pornstar number: ").strip())
            if 1 <= choice <= len(files):
                return files[choice - 1].stem
        except ValueError:
            pass
        log("Invalid selection, try again.", level="warning")


# ============================================================
# SCROLL / LAZY LOAD
# ============================================================


def scroll_to_top(driver):
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)


def ensure_all_thumbnails_loaded(driver, timeout=30):
    """
    Ensures ALL img.thumb elements load real image URLs by
    scrolling EACH image into the viewport individually.
    """

    start_time = time.time()

    imgs = driver.find_elements(By.CSS_SELECTOR, "img.thumb")
    if not imgs:
        return

    for idx, img in enumerate(imgs, start=1):
        # ⏱ safety timeout
        if time.time() - start_time > timeout:
            log(
                f"⚠️ Timeout while loading thumbnails ({idx}/{len(imgs)})",
                level="warning",
            )
            return

        try:
            # 🔑 FORCE image into viewport
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});",
                img,
            )
            time.sleep(0.3)

            # ⏳ wait until src becomes real
            for _ in range(10):
                src = img.get_attribute("src") or ""
                if not src.startswith("data:image"):
                    break
                time.sleep(0.2)

        except Exception:
            continue

    # 🔝 return to top for clean parsing
    scroll_to_top(driver)


# ============================================================
# PAGINATION
# ============================================================


def get_last_page(soup: BeautifulSoup) -> int:
    last = soup.select_one("li.last a[data-parameters]")
    if not last:
        return 1
    for p in last["data-parameters"].split(";"):
        if p.startswith("from:"):
            return int(p.split(":")[1])
    return 1


def go_to_page(driver, page: int):
    wait = WebDriverWait(driver, 15)

    pagination = wait.until(
        EC.presence_of_element_located(
            (By.ID, "list_videos_common_videos_list_pagination")
        )
    )

    links = pagination.find_elements(By.CSS_SELECTOR, "a[data-parameters]")

    target = None
    target_params = None

    for link in links:
        params = link.get_attribute("data-parameters") or ""
        if f"from:{page}" in params:
            target = link
            target_params = params
            break

    if not target or not target_params:
        raise RuntimeError(f"Pagination link not found for page {page}")

    log(f"↪️ Loading page {page} via KVS AJAX", level="info")

    # 🚫 HARD ANTI-AD PROTECTION (strip hijack handlers)
    driver.execute_script(
        """
        arguments[0].removeAttribute('target');
        arguments[0].removeAttribute('onclick');
        arguments[0].removeAttribute('onmousedown');
        """,
        target,
    )

    # 🧠 MANUAL AJAX LOAD (NO CLICK = NO ADS)
    ajax_script = """
    if (typeof window.jQuery !== 'undefined') {
        $.ajax({
            url: window.location.href,
            type: 'GET',
            data: { %s },
            success: function (html) {
                var temp = document.createElement('div');
                temp.innerHTML = html;

                var newList = temp.querySelector('#list_videos_common_videos_list');
                var oldList = document.querySelector('#list_videos_common_videos_list');

                if (newList && oldList) {
                    oldList.innerHTML = newList.innerHTML;
                }
            }
        });
    }
    """ % ",".join(
        f"'{p.split(':')[0]}':'{p.split(':')[1]}'" for p in target_params.split(";")
    )

    driver.execute_script(ajax_script)

    # ⏳ allow DOM to update
    time.sleep(3)


# ============================================================
# PARSING
# ============================================================


def parse_current_page(driver) -> List[Dict]:
    ensure_all_thumbnails_loaded(driver)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []

    for item in soup.select("div.item"):
        img = item.select_one("img.thumb")
        a = item.find("a", href=True)
        title = item.select_one("strong.title")

        if not img or not a or not title:
            continue

        src = img.get("src") or img.get("data-src") or img.get("data-webp")
        if not src or src.startswith("data:image"):
            continue

        results.append(
            {
                "scene_title": title.get_text(strip=True),
                "scene_url": a["href"],
                "thumbnail": {
                    "src": src,
                    "preview_video": img.get("data-preview"),
                },
                "video_meta": {
                    "duration": item.select_one("div.duration")
                    and item.select_one("div.duration").get_text(strip=True),
                    "rating": item.select_one("div.rating")
                    and item.select_one("div.rating").get_text(strip=True),
                    "views": item.select_one("div.views")
                    and item.select_one("div.views").get_text(strip=True),
                },
            }
        )

    return results


# ============================================================
# SCRAPING PIPELINE
# ============================================================


def scrape_all_pages(driver, url: str) -> List[Dict]:
    driver.get(url)
    time.sleep(3)

    ensure_age_verification(driver, logger=logger)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    last_page = get_last_page(soup)
    log(f"📄 Total pages: {last_page}", level="info")

    seen = set()
    all_scenes = []

    for page in range(1, last_page + 1):
        log(f"➡️ Page {page}/{last_page}", level="info")

        if page > 1:
            go_to_page(driver, page)
            scroll_to_top(driver)

        for scene in parse_current_page(driver):
            if scene["scene_url"] not in seen:
                seen.add(scene["scene_url"])
                all_scenes.append(scene)

    return all_scenes


# ============================================================
# ENTRY POINT
# ============================================================


def main():
    driver = create_driver(headless=False)
    try:
        slug = select_pornstar_from_data_folder(JSON_SOURCE_DIR)
        url = f"https://watchporn.to/models/{slug.title()}/"

        log(f"🎯 Selected pornstar: {slug}", level="info")
        log(f"🌐 URL: {url}", level="info")

        scenes = scrape_all_pages(driver, url)

        output = DATA_DIR / f"{slug}.json"
        output.write_text(
            json.dumps(scenes, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        log(f"💾 Saved {len(scenes)} scenes → {output}", level="info")

    finally:
        driver.quit()
        log("👋 Browser closed.", level="info")


if __name__ == "__main__":
    main()

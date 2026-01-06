"""
WatchPorn Pornstar Scenes Scraper (2-Phase)
------------------------------------------

• Phase 1: Collect scene URLs + preview videos (model page)
• Phase 2: Scrape authoritative metadata (detail page)
• Incremental JSON writing (safe for long runs)
• TEST MODE: 1 page, 1 scene
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
# CONFIG (TEST MODE)
# ============================================================

MAX_PAGES = 1  # ⛔ change to None for all pages
MAX_SCENES = None  # ⛔ change to None for all scenes

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
# SHARED HELPERS
# ============================================================

# -------- AGE VERIFICATION --------
AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)


def load_age_verification():
    spec = importlib.util.spec_from_file_location(
        "age_verification", str(AGE_VERIFICATION_PATH)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return getattr(module, "ensure_age_verification", None)


ensure_age_verification = load_age_verification()

# -------- DRIVER --------
DRIVER_SETUP_PATH = PROJECT_ROOT / "scrapers" / "setup" / "driver-setup" / "main.py"
spec = importlib.util.spec_from_file_location("driver_setup", str(DRIVER_SETUP_PATH))
_driver_setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_driver_setup)  # type: ignore[attr-defined]
create_driver = _driver_setup.create_driver

# -------- LOGGER --------
CUSTOM_LOGGER_PATH = PROJECT_ROOT / "scrapers" / "setup" / "custom-logger" / "main.py"
spec = importlib.util.spec_from_file_location("custom_logger", str(CUSTOM_LOGGER_PATH))
_logger_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_logger_utils)  # type: ignore[attr-defined]

log = _logger_utils.log
logger = _logger_utils.CustomLoggerAdapter(log)

# ============================================================
# CLI
# ============================================================


def select_pornstar_from_data_folder(path: Path) -> str:
    files = sorted(f for f in path.iterdir() if f.suffix == ".json")
    log("📂 Available pornstars:", level="info")
    for i, f in enumerate(files, start=1):
        log(f"{i}. {f.stem}", level="info")

    return files[int(input("Select pornstar number: ")) - 1].stem


# ============================================================
# PAGINATION (KVS SAFE)
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
    pagination = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.ID, "list_videos_common_videos_list_pagination")
        )
    )

    for a in pagination.find_elements(By.CSS_SELECTOR, "a[data-parameters]"):
        if f"from:{page}" in (a.get_attribute("data-parameters") or ""):
            driver.execute_script(
                """
                arguments[0].removeAttribute('target');
                arguments[0].removeAttribute('onclick');
                arguments[0].removeAttribute('onmousedown');
                """,
                a,
            )
            driver.execute_script(
                """
                $.ajax({
                  url: window.location.href,
                  type: 'GET',
                  data: {%s},
                  success: function(html){
                    let t=document.createElement('div');
                    t.innerHTML=html;
                    document.querySelector('#list_videos_common_videos_list').innerHTML =
                      t.querySelector('#list_videos_common_videos_list').innerHTML;
                  }
                });
                """
                % ",".join(
                    f"'{x.split(':')[0]}':'{x.split(':')[1]}'"
                    for x in a.get_attribute("data-parameters").split(";")
                )
            )
            time.sleep(3)
            return


# ============================================================
# PHASE 1 — COLLECT SCENE URL + PREVIEW
# ============================================================


def collect_scene_index(driver, url: str) -> List[Dict]:
    driver.get(url)
    time.sleep(3)
    ensure_age_verification(driver, logger=logger)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    last_page = min(get_last_page(soup), MAX_PAGES or 999)

    results = []

    for page in range(1, last_page + 1):
        log(f"➡️ Index page {page}", level="info")
        if page > 1:
            go_to_page(driver, page)

        for item in soup.select("div.item"):
            img = item.select_one("img.thumb")
            a = item.find("a", href=True)
            if not img or not a:
                continue

            preview = img.get("data-preview")
            if not preview:
                video = item.select_one("video")
                preview = video.get("src") if video else None

            results.append(
                {
                    "scene_url": a["href"],
                    "preview_video": preview,
                }
            )

            if MAX_SCENES and len(results) >= MAX_SCENES:
                return results

    return results


# ============================================================
# PHASE 2 — DETAIL PAGE
# ============================================================


def scrape_scene_details(driver, scene: Dict) -> Dict:
    driver.get(scene["scene_url"])
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # -------- TITLE --------
    title_tag = soup.select_one("div.headline h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    # -------- THUMBNAIL --------
    og_image = soup.select_one('meta[property="og:image"]')
    thumbnail = og_image["content"] if og_image else None

    description = None
    categories = []
    tags = []
    models = []

    # -------- META DATA --------
    meta_data = {
        "duration": None,
        "views": None,
        "submitted": None,
        "rating_percent": None,
        "rating_votes": None,
    }

    info = soup.select_one("#tab_video_info")

    if info:
        for item in info.select("div.item"):
            label = item.get_text(" ", strip=True).lower()

            # ---- Description ----
            if label.startswith("description"):
                em = item.find("em")
                if em:
                    description = em.get_text(" ", strip=True)

            # ---- Categories ----
            elif label.startswith("categories"):
                categories = [a.get_text(strip=True) for a in item.find_all("a")]

            # ---- Tags ----
            elif label.startswith("tags"):
                tags = [a.get_text(strip=True) for a in item.find_all("a")]

            # ---- Models ----
            elif label.startswith("models"):
                models = [a.get_text(strip=True) for a in item.find_all("a")]

            # ---- Duration / Views / Submitted ----
            for span in item.find_all("span"):
                span_label = span.get_text(strip=True).lower()
                em = span.find("em")
                value = em.get_text(strip=True) if em else None

                if span_label.startswith("duration"):
                    meta_data["duration"] = value
                elif span_label.startswith("views"):
                    meta_data["views"] = value
                elif span_label.startswith("submitted"):
                    meta_data["submitted"] = value

    # -------- RATING --------
    rating = soup.select_one("div.rating")
    if rating:
        voters = rating.select_one("span.voters")
        scale = rating.select_one("span.scale")

        if voters and "%" in voters.get_text():
            meta_data["rating_percent"] = (
                voters.get_text(strip=True).split("%", 1)[0] + "%"
            )

        if scale and scale.has_attr("data-votes"):
            try:
                meta_data["rating_votes"] = int(scale["data-votes"])
            except ValueError:
                pass

    return {
        "scene_url": scene["scene_url"],
        "scene_title": title,
        "thumbnail": thumbnail,
        "preview_video": scene["preview_video"],
        "description": description,
        "categories": categories,
        "tags": tags,  # ✅ ADDED
        "models": models,
        "meta_data": meta_data,
    }


# ============================================================
# MAIN
# ============================================================


def main():
    slug = select_pornstar_from_data_folder(JSON_SOURCE_DIR)
    model_url = f"https://watchporn.to/models/{slug.title()}/"

    log(f"🎯 Selected pornstar: {slug}", level="info")
    log(f"🌐 URL: {model_url}", level="info")

    driver = create_driver(headless=False)
    try:
        scenes = collect_scene_index(driver, model_url)
        log(f"🔗 Collected {len(scenes)} scene(s)", level="info")

        output = DATA_DIR / f"{slug}.json"
        data = []

        for i, scene in enumerate(scenes, start=1):
            log(f"🎬 Scraping scene {i}", level="info")
            record = scrape_scene_details(driver, scene)
            data.append(record)

            # Incremental save
            output.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            log(f"💾 Saved scene {i}", level="success")

    finally:
        driver.quit()
        log("👋 Browser closed", level="info")


if __name__ == "__main__":
    main()

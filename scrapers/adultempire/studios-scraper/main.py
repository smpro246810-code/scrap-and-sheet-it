import json
import logging
import sys
import time
from pathlib import Path
import importlib.util
from typing import List, Dict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# PROJECT SETUP
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

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
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# CONFIG (ADD MORE CATEGORIES HERE)
# ============================================================

STUDIO_CATEGORIES = {
    "bluray": {
        "url": "https://www.adultempire.com/all-blu-ray-studios.html?letter=C",
        "output": "bluray-studios.json",
        "count_field": "num_blurays",
    },
    "clips": {
        "url": "https://www.adultempire.com/all-clips-studios.html?letter=C",
        "output": "clips-studios.json",
        "count_field": "num_clips",
    },
    "dvds": {
        "url": "https://www.adultempire.com/all-dvds-studios.html?letter=C",
        "output": "dvd-studios.json",
        "count_field": "num_dvds",
    },
    "vods": {
        "url": "https://www.adultempire.com/all-vods-studios.html?letter=C",
        "output": "vod-studios.json",
        "count_field": "num_vods",
    },
}


# ============================================================
# LOGGING
# ============================================================


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


# ============================================================
# SELENIUM
# ============================================================


def create_driver(visible: bool = True) -> webdriver.Chrome:
    options = Options()
    if not visible:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def deep_scroll_until_stable(driver, max_scrolls=20, pause=0.6, logger=None):
    last_height = driver.execute_script("return document.body.scrollHeight")

    for i in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

        new_height = driver.execute_script("return document.body.scrollHeight")

        if logger:
            logger.info(f"🔽 Scroll {i+1}, height={new_height}")

        if new_height == last_height:
            if logger:
                logger.info("✅ No new content loaded — stopping scroll")
            break

        last_height = new_height


# ============================================================
# PARSER (GENERIC)
# ============================================================


def parse_studios(
    html: str,
    count_field: str,
    logger: logging.Logger,
) -> List[Dict]:

    soup = BeautifulSoup(html, "html.parser")
    studios: List[Dict] = []

    containers = soup.select("ul.cat-list")
    links = []
    for container in containers:
        links.extend(container.select("li > a"))

    logger.info(f"📦 Found {len(links)} studio entries")

    for link in links:
        parent_li = link.find_parent("li")
        if not parent_li:
            continue

        title = link.get_text(strip=True)
        href = link.get("href") or ""

        count = 0
        count_tag = parent_li.find("small")
        if count_tag:
            try:
                count = int(count_tag.get_text(strip=True).strip("()").replace(",", ""))
            except ValueError:
                pass

        studios.append(
            {
                "title": title,
                "url": f"https://www.adultempire.com{href}",
                count_field: count,
            }
        )

    return studios


# ============================================================
# SCRAPER PIPELINE (SINGLE SESSION)
# ============================================================


def scrape_all_categories(
    categories: Dict[str, Dict],
    logger: logging.Logger,
):
    driver = create_driver(visible=True)

    try:
        for index, (category, config) in enumerate(categories.items()):
            logger.info(f"🚀 Scraping {category.upper()} studios")

            driver.get(config["url"])

            # Age verification usually needed only once
            if index == 0:
                ensure_age_verification(driver, logger)
                time.sleep(3)

            time.sleep(3)
            deep_scroll_until_stable(driver, logger=logger)

            html = driver.page_source
            data = parse_studios(html, config["count_field"], logger)

            output_path = DATA_DIR / config["output"]
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"✅ Saved {len(data)} → {output_path}")

    finally:
        driver.quit()


# ============================================================
# ENTRY POINT
# ============================================================


def main():
    logger = setup_logger()
    scrape_all_categories(STUDIO_CATEGORIES, logger)


if __name__ == "__main__":
    main()

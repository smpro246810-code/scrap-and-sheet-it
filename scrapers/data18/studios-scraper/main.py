"""
Data18 Studios Scraper
---------------------

• Single Selenium session
• Safe age-verification handling
• Lazy-load scrolling + pagination
• Robust parsing with defaults
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
import sys
import time
import logging
import importlib.util
from pathlib import Path
from typing import Dict, List, Union

# ============================================================
# THIRD-PARTY LIBS
# ============================================================

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# PROJECT SETUP & PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://www.data18.com/studios"
OUTPUT_FILE = DATA_DIR / "data18-studios.json"

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)

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


ensure_age_verification = (
    load_age_verification() or ensure_age_verification_fallback
)

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

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

# ============================================================
# SCROLLING (LAZY LOAD SUPPORT)
# ============================================================

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
# PARSING (PURE FUNCTION)
# ============================================================

def parse_studio_page(html: str, logger: logging.Logger) -> List[Dict]:
    """
    Parse a Data18 studios listing page.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = soup.select("#listing_results > a")
    results: List[Dict] = []

    for entry in entries:
        try:
            url = entry.get("href")
            title_tag = entry.find("b")
            title = title_tag.get_text(strip=True) if title_tag else None

            # Extract number of scenes
            text = entry.get_text(separator=" ", strip=True)
            num_scenes = 0
            if "----" in text:
                after_dash = text.split("----", 1)[1].strip()
                first_token = after_dash.split(" ", 1)[0]
                num_scenes = int(first_token) if first_token.isdigit() else 0

            results.append(
                {
                    "title": title,
                    "url": url,
                    "num_scenes": num_scenes,
                }
            )

        except Exception as e:
            logger.warning(f"⚠️ Failed to parse entry: {e}")

    return results

# ============================================================
# SCRAPING PIPELINE (PAGINATION)
# ============================================================

def scrape_all_studios(driver: webdriver.Chrome, logger: logging.Logger):
    results = []
    page = 1

    while True:
        logger.info(f"📝 Scraping page {page}")

        # Adaptive scrolling (replaces fixed scrolling)
        deep_scroll_until_stable(driver, logger=logger)

        html = driver.page_source
        page_data = parse_studio_page(html, logger)
        logger.info(f"🔍 Page {page}: {len(page_data)} studios")

        results.extend(page_data)

        try:
            next_btn = driver.find_element(
                By.XPATH,
                "//div[contains(@id, 'spagea') and contains(., 'Next')]",
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
            driver.execute_script("arguments[0].click();", next_btn)
            page += 1
            time.sleep(4)
        except NoSuchElementException:
            logger.info("✅ No more pages found.")
            break
        except Exception as e:
            logger.warning(f"⚠️ Failed to navigate to next page: {e}")
            break

    return results

# ============================================================
# OUTPUT
# ============================================================

def save_json(data: List[Dict], path: Path, logger: logging.Logger):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(f"💾 Saved {len(data)} records → {path}")

# ============================================================
# ENTRY POINT
# ============================================================

def main():
    logger.info(f"🌐 Starting Data18 scraper at: {TARGET_URL}")

    driver = create_driver(headless=False)
    try:
        driver.get(TARGET_URL)
        time.sleep(3)

        ensure_age_verification(driver, logger)
        time.sleep(2)

        studios = scrape_all_studios(driver, logger)

        logger.info(f"🎉 Finished! Total studios scraped: {len(studios)}")
        save_json(studios, OUTPUT_FILE, logger)

    except Exception as e:
        logger.error(f"🚨 Scraper error: {e}", exc_info=True)
    finally:
        driver.quit()
        logger.info("👋 Browser closed. Session ended.")

if __name__ == "__main__":
    main()

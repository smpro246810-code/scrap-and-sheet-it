"""
Data18 Male Pornstars Scraper
----------------------------

• Single Selenium session
• Safe age-verification handling
• Adaptive scrolling (scroll-until-stable)
• Robust parsing with retries
• Pagination via "Next" button
• Single-file, modular, scalable design
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
from typing import Any, Dict, List, Optional, Union
import inquirer
from datetime import datetime


# ============================================================
# THIRD-PARTY LIBS
# ============================================================

from bs4 import BeautifulSoup, Tag
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

TARGET_URL = "https://www.data18.com/names/pornstars-male"

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)

SCRAPE_TARGETS = {
    "male": {
        "label": "Male pornstars only",
        "url": "https://www.data18.com/names/pornstars-male",
        "output": DATA_DIR / "male-pornstars.json",
        "expected_per_page": 40,
    },
    "all": {
        "label": "All pornstars",
        "url": "https://www.data18.com/names/pornstars",
        "output": DATA_DIR / "all-pornstars.json",
        "expected_per_page": 40,
    },
}


def select_scrape_mode() -> str:
    """
    Interactive terminal menu for selecting scrape mode.
    """
    choices = [(cfg["label"], key) for key, cfg in SCRAPE_TARGETS.items()]

    answer = inquirer.prompt(
        [
            inquirer.List(
                "mode",
                message="Select scraping mode",
                choices=choices,
            )
        ]
    )

    if not answer:
        raise RuntimeError("No option selected. Aborting.")

    return answer["mode"]


def load_existing_data(path: Path):
    """
    Load existing scrape state if file exists.
    """
    if not path.exists():
        return {
            "meta": {
                "last_page": 0,
                "updated_at": None,
            },
            "data": [],
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "meta" in payload and "data" in payload:
            return payload
    except Exception:
        pass

    return {
        "meta": {
            "last_page": 0,
            "updated_at": None,
        },
        "data": [],
    }


def save_checkpoint(
    path: Path,
    data: List[Dict[str, Any]],
    page: int,
    mode: str,
    logger: logging.Logger,
):
    payload = {
        "meta": {
            "mode": mode,
            "last_page": page,
            "updated_at": datetime.utcnow().isoformat(),
        },
        "data": data,
    }

    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(f"💾 Checkpoint saved (page {page}) → {path}")


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

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


# ============================================================
# SCROLLING (ADAPTIVE)
# ============================================================


def deep_scroll_until_stable(
    driver: webdriver.Chrome,
    max_scrolls: int = 20,
    pause: Union[int, float] = 0.6,
    logger: Optional[logging.Logger] = None,
):
    """
    Scrolls until page height stops increasing (lazy-load safe).
    """
    last_height = driver.execute_script("return document.body.scrollHeight")

    for i in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

        new_height = driver.execute_script("return document.body.scrollHeight")

        if logger:
            logger.info(f"🔽 Scroll {i + 1}, height={new_height}")

        if new_height == last_height:
            if logger:
                logger.info("✅ No new content loaded — stopping scroll")
            break

        last_height = new_height


# ============================================================
# PARSER (PURE FUNCTION)
# ============================================================


def parse_one_page(html: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Parse one page of male pornstar listings.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries = soup.select(".boxep1 > div > div")
    results: List[Dict[str, Any]] = []

    for entry in entries:
        try:
            name_tag: Optional[Tag] = entry.select_one("div.gen12.bold")
            if not name_tag:
                continue

            name = name_tag.get_text(strip=True)

            a_tag: Optional[Tag] = entry.find("a", href=True)
            profile_url = a_tag.get("href") if a_tag else ""

            img_tag: Optional[Tag] = entry.find("img")
            image_url = img_tag.get("src") if img_tag else ""
            if "no_prev_120.gif" in image_url:
                image_url = ""

            stats_tag: Optional[Tag] = entry.find("p", class_="gen11")
            stats_text = stats_tag.get_text(strip=True) if stats_tag else ""

            scenes = 0
            movies = 0

            if "Scenes" in stats_text:
                try:
                    scenes = int(stats_text.split("Scenes")[0].strip())
                except ValueError:
                    pass

            if "Movies" in stats_text:
                try:
                    movies = int(stats_text.split("Movies")[1].strip("[] ").strip())
                except (ValueError, IndexError):
                    pass

            results.append(
                {
                    "name": name,
                    "profile_url": profile_url,
                    "image_url": image_url,
                    "num_scenes": scenes,
                    "num_movies": movies,
                }
            )

        except Exception as e:
            logger.warning(f"⚠️ Error parsing entry: {e}", exc_info=True)

    return results


# ============================================================
# SCRAPING PIPELINE (RETRIES + PAGINATION)
# ============================================================


def scrape_all_male_pornstars(
    driver: webdriver.Chrome,
    logger: logging.Logger,
    output_file: Path,
    mode: str,
    retries_per_page: int = 3,
    expected_per_page: int = 40,
) -> List[Dict[str, Any]]:

    state = load_existing_data(output_file)
    results: List[Dict[str, Any]] = state["data"]
    page = state["meta"].get("last_page", 0) + 1

    logger.info(f"▶️ Resuming from page {page}")

    # Move browser forward to last saved page
    for _ in range(1, page):
        try:
            next_btn = driver.find_element(
                By.XPATH,
                "//div[contains(@id, 'spagea') and contains(., 'Next')]",
            )
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(3)
        except Exception:
            logger.warning("⚠️ Could not fast-forward pages")
            break

    while True:
        page_data: List[Dict[str, Any]] = []

        for attempt in range(1, retries_per_page + 1):
            logger.info(f"📝 Scraping page {page} (attempt {attempt})")

            deep_scroll_until_stable(driver, logger=logger)

            html = driver.page_source
            page_data = parse_one_page(html, logger)
            count = len(page_data)

            logger.info(f"🔍 Page {page} extracted {count} stars")

            if count == expected_per_page or "Next" not in html:
                break

            if attempt < retries_per_page:
                time.sleep(2)

        if not page_data:
            logger.warning("⚠️ No data extracted, stopping.")
            break

        results.extend(page_data)

        # 🔥 SAVE CHECKPOINT AFTER EACH PAGE
        save_checkpoint(
            output_file,
            results,
            page,
            mode,
            logger,
        )

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
            logger.info("✅ No more pages found. Scraping complete.")
            break

    return results


# ============================================================
# OUTPUT
# ============================================================


def save_json(data: List[Dict[str, Any]], path: Path, logger: logging.Logger):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(f"💾 Data saved to {path}")


# ============================================================
# ENTRY POINT
# ============================================================


def main():
    try:
        mode = select_scrape_mode()
    except Exception as e:
        logger.error(f"❌ {e}")
        return

    target = SCRAPE_TARGETS[mode]
    url = target["url"]
    output_file = target["output"]
    expected_per_page = target["expected_per_page"]

    logger.info(f"🌐 Scrape mode: {target['label']}")
    logger.info(f"🔗 Target URL: {url}")

    driver = create_driver(headless=False)
    try:
        driver.get(url)
        time.sleep(3)

        ensure_age_verification(driver, logger)
        time.sleep(2)

        data = scrape_all_male_pornstars(
            driver,
            logger,
            output_file=output_file,
            mode=mode,
            expected_per_page=expected_per_page,
        )

        logger.info(f"🎉 Finished! Total scraped: {len(data)}")
        save_json(data, output_file, logger)

    except Exception as e:
        logger.error(f"🚨 Scraper error: {e}", exc_info=True)

    finally:
        driver.quit()
        logger.info("👋 Browser closed. Session ended.")


if __name__ == "__main__":
    main()

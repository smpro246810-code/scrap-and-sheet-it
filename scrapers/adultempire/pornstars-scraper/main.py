"""
AdultEmpire Performer Scraper
-----------------------------

• Single Selenium session
• Interactive CLI (Sex filter + scrape mode)
• First-page or full pagination scrape
• Unified logging (console + file)
• Safe age-verification handling
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import json
import sys
import time
import importlib.util
from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin

# ============================================================
# THIRD-PARTY LIBS
# ============================================================

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from InquirerPy import inquirer
from colorama import Fore, Style, init

# ============================================================
# PROJECT SETUP
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

BASE_LIST_URL = "https://www.adultempire.com/hottest-pornstars.html?pageSize=100"
LOG_FILE = LOG_DIR / "logs.log"

init(autoreset=True)

# ============================================================
# LOGGING
# ============================================================

def log(message: str, level: str = "info", console: bool = True):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    tag = f"[{level.upper()}]"
    colors = {
        "info": Fore.CYAN + Style.BRIGHT,
        "success": Fore.GREEN + Style.BRIGHT,
        "warning": Fore.YELLOW + Style.BRIGHT,
        "error": Fore.RED + Style.BRIGHT,
    }

    if console:
        print(f"{colors.get(level, Fore.WHITE)}{timestamp} {tag:<10}{Style.RESET_ALL} {message}")

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {tag} {message}\n")


class LoggerAdapter:
    """Compatibility adapter for age-verification module."""
    def info(self, msg): log(msg, "info")
    def warning(self, msg): log(msg, "warning")
    def error(self, msg): log(msg, "error")


# ============================================================
# AGE VERIFICATION (SAFE DYNAMIC LOAD)
# ============================================================

AGE_VERIFICATION_PATH = (
    PROJECT_ROOT / "scrapers" / "setup" / "age-verification" / "main.py"
)

def load_age_verification():
    if not AGE_VERIFICATION_PATH.exists():
        return None

    spec = importlib.util.spec_from_file_location(
        "age_verification", str(AGE_VERIFICATION_PATH)
    )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore
        return getattr(module, "ensure_age_verification", None)
    except Exception:
        return None


def ensure_age_verification_fallback(driver, logger=None):
    if logger:
        logger.info("Age verification unavailable; skipping.")


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
# PARSERS (PURE FUNCTIONS)
# ============================================================

def parse_performers(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.col-xs-6.col-sm-4.col-md-3.col-lg-2.m-b-2")
    results = []

    for card in cards:
        a = card.find("a", href=True, label=True)
        if not a:
            continue

        img = a.find("img", alt=True)
        results.append({
            "name": a["label"].strip(),
            "profile_url": f"https://www.adultempire.com{a['href']}",
            "image_url": img["src"] if img else ""
        })

    return sorted(results, key=lambda x: x["name"].lower())


def extract_sex_filters(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    for block in soup.select("div.refine-set"):
        header = block.find("h4")
        if header and header.get_text(strip=True) == "Sex":
            return {
                a["title"]: a["href"]
                for a in block.select("a[title][href]")
            }
    return {}

# ============================================================
# SCRAPERS
# ============================================================

def scrape_first_page(driver, url: str) -> List[Dict]:
    log(f"🌐 Scraping first page: {url}")
    driver.get(url)
    time.sleep(2)
    return parse_performers(driver.page_source)


def scrape_all_pages(driver, base_url: str) -> List[Dict]:
    results = []
    page = 1

    while True:
        url = f"{base_url}&page={page}"
        log(f"🌐 Scraping page {page}")
        driver.get(url)
        time.sleep(2)

        data = parse_performers(driver.page_source)
        if not data:
            log("✅ No more pages found", "success")
            break

        results.extend(data)
        page += 1

    return results


def clear_filter(driver) -> bool:
    try:
        refined = driver.find_element(By.ID, "RefinedBy")
        href = refined.find_element(By.TAG_NAME, "a").get_attribute("href")
        driver.get(href)
        time.sleep(2)
        return True
    except Exception:
        return False

# ============================================================
# STORAGE
# ============================================================

def save_json(data: List[Dict], filename: str):
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"💾 Saved {len(data)} records → {path}", "success")

# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def main():
    log("🎬 AdultEmpire Performer Scraper Started")

    driver = create_driver(headless=False)
    logger_adapter = LoggerAdapter()

    try:
        driver.get(BASE_LIST_URL)
        ensure_age_verification(driver, logger_adapter)

        while True:
            filters = extract_sex_filters(driver.page_source)
            if not filters:
                log("❌ Failed to load sex filters", "error")
                break

            choice = inquirer.select(
                message="Select category:",
                choices=list(filters.keys()) + ["Quit"],
                pointer="👉"
            ).execute()

            if choice == "Quit":
                break

            mode = inquirer.select(
                message="Scrape mode:",
                choices=["First page", "All pages"],
                pointer="👉"
            ).execute()

            start_url = urljoin(BASE_LIST_URL, filters[choice])

            data = (
                scrape_first_page(driver, start_url)
                if mode == "First page"
                else scrape_all_pages(driver, start_url)
            )

            save_json(data, f"{choice.lower()}-pornstars.json")

            if not clear_filter(driver):
                driver.get(BASE_LIST_URL)
                time.sleep(2)

    except Exception as e:
        log(f"🚨 Scraper crashed: {e}", "error")
    finally:
        driver.quit()
        log("👋 Browser closed. Session ended.")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

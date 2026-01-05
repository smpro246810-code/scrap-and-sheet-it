"""
Unified Age Verification Helper for Selenium
===========================================

• Built-in site profiles (AdultEmpire, Data18)
• Runtime-extensible via profile registry
• Generic fallback for unknown sites
• Automatic screenshot + HTML dump on failure

Public API:
    ensure_age_verification(driver, logger=None, url=None) -> bool
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple

# ============================================================
# SELENIUM
# ============================================================

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ============================================================
# DEBUGGING / FORENSICS
# ============================================================


def save_debug_capture(
    driver,
    label: str = "age_gate_unresolved",
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Save screenshot + HTML dump for unresolved age gates.
    """
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    debug_dir = Path("debug")
    debug_dir.mkdir(exist_ok=True)

    screenshot_path = debug_dir / f"{label}_{timestamp}.png"
    html_path = debug_dir / f"{label}_{timestamp}.html"

    try:
        driver.save_screenshot(screenshot_path)
        logger and logger.info(f"📸 Screenshot saved: {screenshot_path}")
    except Exception as e:
        logger and logger.warning(f"⚠️ Screenshot failed: {e}")

    try:
        html_path.write_text(driver.page_source or "", encoding="utf-8")
        logger and logger.info(f"📝 HTML dump saved: {html_path}")
    except Exception as e:
        logger and logger.warning(f"⚠️ HTML dump failed: {e}")


# ============================================================
# DATA MODEL
# ============================================================


@dataclass
class SiteProfile:
    """
    Declarative definition of an age-gate profile.
    """

    domain_pattern: re.Pattern
    detect_texts: List[str] = field(default_factory=list)
    click_targets: List[Tuple[str, str]] = field(default_factory=list)
    post_click_sleep: float = 3.0
    custom_handler: Optional[Callable] = None


# ============================================================
# PROFILE REGISTRY
# ============================================================

_SITE_PROFILES: List[SiteProfile] = []


def register_site_profile(profile: SiteProfile) -> None:
    _SITE_PROFILES.append(profile)


# ============================================================
# BUILT-IN SITE PROFILES
# ============================================================

register_site_profile(
    SiteProfile(
        domain_pattern=re.compile(r"(?:^|\.)adultempire\.com$", re.I),
        detect_texts=[
            "agree to terms and enter the site",
            "age confirmation",
            "i agree - enter",
            "enter the site",
        ],
        click_targets=[
            (By.ID, "ageConfirmationButton"),
            (By.XPATH, "//button[@id='ageConfirmationButton']"),
            (By.XPATH, "//button[contains(., 'Enter')]"),
            (By.XPATH, "//button[contains(., 'ENTER')]"),
        ],
        post_click_sleep=4.0,
    )
)

register_site_profile(
    SiteProfile(
        domain_pattern=re.compile(r"(?:^|\.)data18\.com$", re.I),
        detect_texts=[
            "adults only",
            "enter - data18.com",
            "age verification",
        ],
        click_targets=[
            (By.XPATH, "//button[contains(., 'ENTER - data18.com')]"),
            (By.XPATH, "//a[contains(., 'ENTER - data18.com')]"),
            (By.XPATH, "//button[contains(., 'Enter')]"),
            (By.XPATH, "//button[contains(., 'ENTER')]"),
        ],
        post_click_sleep=5.0,
    )
)

# ============================================================
# GENERIC FALLBACK DEFINITIONS
# ============================================================

_GENERIC_TEXT_MARKERS = [
    "adults only",
    "age verification",
    "are you 18",
    "over 18",
    "you must be 18",
    "agree to terms",
    "enter site",
    "enter the site",
    "age confirm",
    "age gate",
]

_GENERIC_CLICK_XPATHS = [
    "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'enter')]",
    "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'enter')]",
    "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'agree')]",
    "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'agree')]",
    "//button[contains(., 'Yes') or contains(., 'YES')]",
    "//a[contains(., 'Yes') or contains(., 'YES')]",
    "//button[contains(., '18') or contains(., 'eighteen')]",
    "//a[contains(., '18') or contains(., 'eighteen')]",
]

# ============================================================
# INTERNAL UTILITIES
# ============================================================


def _domain_from_url(url: str) -> str:
    match = re.search(r"://([^/]+)", url)
    return match.group(1).split(":")[0].lower() if match else ""


def _page_contains_any_text(driver, texts: List[str]) -> bool:
    page = (driver.page_source or "").lower()
    return any(text in page for text in texts)


def _try_click_targets(
    driver,
    logger: logging.Logger,
    targets: List[Tuple[str, str]],
    timeout: float = 3.0,
) -> bool:
    wait = WebDriverWait(driver, timeout)
    for by, selector in targets:
        try:
            element = wait.until(EC.element_to_be_clickable((by, selector)))
            driver.execute_script("arguments[0].click();", element)
            logger.info(f"✅ Clicked age-gate target: {by} → {selector}")
            return True
        except Exception as e:
            logger.debug(f"❌ Failed click {by} → {selector}: {e}")
    return False


def _generic_fallback(driver, logger: logging.Logger) -> bool:
    if not _page_contains_any_text(driver, _GENERIC_TEXT_MARKERS):
        return False

    logger.info("🔁 Running generic age-gate fallback...")
    if _try_click_targets(
        driver,
        logger,
        [(By.XPATH, xp) for xp in _GENERIC_CLICK_XPATHS],
    ):
        time.sleep(3)
        return True

    return False


# ============================================================
# PUBLIC API
# ============================================================


def ensure_age_verification(
    driver,
    logger: Optional[logging.Logger] = None,
    url: Optional[str] = None,
) -> bool:
    """
    Ensures age verification gates are cleared if present.

    Returns:
        True  → Gate cleared or not present
        False → Gate detected but could not be cleared
    """
    logger = logger or logging.getLogger(__name__)
    current_url = url or driver.current_url
    domain = _domain_from_url(current_url)

    logger.info("🔍 Checking for age verification gate...")

    # ---------- Profile-based detection ----------
    for profile in _SITE_PROFILES:
        if profile.domain_pattern.search(domain):
            if not _page_contains_any_text(driver, profile.detect_texts):
                logger.info("✅ No age gate detected (profile match).")
                return True

            logger.info("🧩 Age gate detected — applying site profile...")
            if _try_click_targets(driver, logger, profile.click_targets):
                time.sleep(profile.post_click_sleep)
                return True

            logger.warning("⚠️ Profile failed; falling back to generic logic.")

    # ---------- Generic fallback ----------
    if _generic_fallback(driver, logger):
        return True

    # ---------- Failure handling ----------
    if _page_contains_any_text(driver, _GENERIC_TEXT_MARKERS):
        logger.error("❌ Age gate unresolved. Saving debug artifacts.")
        save_debug_capture(driver, logger=logger)
        return False

    logger.info("✅ No age verification detected.")
    return True


# ============================================================
# END OF FILE
# ============================================================

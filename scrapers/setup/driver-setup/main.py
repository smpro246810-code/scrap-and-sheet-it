"""
Common Scraper Utilities
========================

• Standardized logging setup
• Reusable Selenium Chrome driver factory
• Safe defaults for scraping environments
• Single-file, drop-in utility

Usage:
    logger = setup_logger("adult_scraper", "adult.log")
    driver = create_driver(headless=True)
"""

# ============================================================
# STANDARD LIBS
# ============================================================

import logging
from typing import Optional

# ============================================================
# THIRD-PARTY
# ============================================================

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# LOGGING
# ============================================================

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DEFAULT_LOG_LEVEL = logging.INFO


def setup_logger(
    name: str = "scraper",
    log_file: str = "scraper.log",
    level: int = DEFAULT_LOG_LEVEL,
    formatter: Optional[logging.Formatter] = None,
) -> logging.Logger:
    """
    Create or return a configured logger.

    • File + console output
    • Prevents duplicate handlers
    • Safe to call multiple times

    :param name: Logger name
    :param log_file: Log file path
    :param level: Logging level
    :param formatter: Optional custom formatter
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger  # Already configured

    fmt = formatter or logging.Formatter(DEFAULT_LOG_FORMAT)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ============================================================
# SELENIUM DRIVER
# ============================================================

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.6613.137 Safari/537.36"
)


def create_driver(
    headless: bool = True,
    user_agent: str = DEFAULT_USER_AGENT,
    window_size: str = "1920,1080",
) -> webdriver.Chrome:
    """
    Create a configured Chrome WebDriver.

    :param headless: Run browser in headless mode
    :param user_agent: Custom user agent
    :param window_size: Browser window size
    """
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--window-size={window_size}")
    options.add_argument(f"user-agent={user_agent}")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
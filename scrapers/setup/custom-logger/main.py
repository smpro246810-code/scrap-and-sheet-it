"""
Lightweight Unified Logger
==========================

• Colored console output
• Persistent file logging
• Simple API (log / console_log)
• Extensible levels and formatting

Usage:
    log("Scraping started")
    log("Saved file successfully", level="success")
    log("Retrying page", level="warning")
    log("Fatal error occurred", level="error")
"""

from pathlib import Path
from datetime import datetime
from typing import Dict
from colorama import Fore, Style, init

# ============================================================
# INITIALIZATION
# ============================================================

init(autoreset=True)

# ============================================================
# PATHS
# ============================================================

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "logs.log"

# ============================================================
# CONFIGURATION
# ============================================================

LOG_COLORS: Dict[str, str] = {
    "info": Fore.CYAN + Style.BRIGHT,
    "success": Fore.GREEN + Style.BRIGHT,
    "warning": Fore.YELLOW + Style.BRIGHT,
    "error": Fore.LIGHTRED_EX + Style.BRIGHT,
}

LOG_TAGS: Dict[str, str] = {
    "info": "[INFO]",
    "success": "[SUCCESS]",
    "warning": "[WARNING]",
    "error": "[ERROR]",
}

DEFAULT_LEVEL = "info"

# ============================================================
# INTERNAL UTILITIES
# ============================================================


def _timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _format_console(message: str, level: str) -> str:
    color = LOG_COLORS.get(level, "")
    tag = LOG_TAGS.get(level, "[LOG]")
    return f"{color}{_timestamp()} {tag:<12} {Style.RESET_ALL}{message}"


def _format_file(message: str, level: str) -> str:
    tag = LOG_TAGS.get(level, "[LOG]")
    return f"[{_timestamp()}] {tag} {message}"


# ============================================================
# PUBLIC API
# ============================================================


def log(message: str, level: str = DEFAULT_LEVEL) -> None:
    """
    Log message to console (colored) and file (plain text).

    :param message: Message to log
    :param level: info | success | warning | error
    """
    console_line = _format_console(message, level)
    file_line = _format_file(message, level)

    print(console_line)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(file_line + "\n")


def console_log(message: str, level: str = DEFAULT_LEVEL) -> None:
    """
    Log message to console only (no file write).
    """
    color = LOG_COLORS.get(level, "")
    tag = LOG_TAGS.get(level, "[LOG]")
    print(f"{color}{tag:<12}{Style.RESET_ALL}{message}")


# ============================================================
# LOGGER ADAPTER (logging.Logger compatibility)
# ============================================================


class CustomLoggerAdapter:
    """
    Adapter that makes the custom `log()` function compatible
    with logging.Logger-style APIs.

    This allows usage like:
        logger.info("message")
        logger.warning("message")
        logger.error("message")

    while still using the custom logger internally.
    """

    def __init__(self, log_func):
        self._log = log_func

    def info(self, message: str):
        self._log(message, level="info")

    def success(self, message: str):
        self._log(message, level="success")

    def warning(self, message: str):
        self._log(message, level="warning")

    def error(self, message: str):
        self._log(message, level="error")

    def debug(self, message: str):
        # Map debug → info (you can change later)
        self._log(message, level="info")

    def exception(self, message: str):
        self._log(message, level="error")

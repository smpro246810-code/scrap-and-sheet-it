"""
Title Formatting Utility
========================

• Cleans invalid characters
• Applies title casing with advanced rules
• Config-driven exceptions (uppercase, small words, exact words)
• Handles roman numerals, ordinals, hyphenation, possessives

"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List

# ============================================================
# PATHS & CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

CONFIG_PATH = BASE_DIR / "utils" / "config.json"

# ============================================================
# REGEX PATTERNS
# ============================================================

_ROMAN_RE = re.compile(
    r"^(?=[MDCLXVI])(M{0,4}(CM|CD|D?C{0,3})"
    r"(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))$",
    re.IGNORECASE,
)

_ORDINAL_RE = re.compile(r"^(\d+)(ST|ND|RD|TH)$", re.IGNORECASE)

# ============================================================
# CONFIG LOADING
# ============================================================

def load_config(path: Path = CONFIG_PATH) -> Dict[str, List[str]]:
    """
    Load formatting rules from config.json and normalize lookups.
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg["_always_uppercase_norm"] = {w.lower() for w in cfg.get("always_uppercase", [])}
    cfg["_small_words_norm"] = {w.lower() for w in cfg.get("small_words", [])}
    cfg["_exact_map"] = {w.lower(): w for w in cfg.get("exact_words", [])}

    return cfg


_CFG = load_config()

# ============================================================
# CLEANING UTILITIES
# ============================================================

def remove_invalid_chars(text: str) -> str:
    """
    Remove filesystem-invalid characters and normalize spacing.
    """
    text = (text or "").replace("\u00A0", " ")
    text = text.replace(":", " -").replace("/", "&")
    text = re.sub(r'[\\:*?"<>|]', "", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


def _contains_letters_and_digits(word: str) -> bool:
    return bool(re.search(r"[A-Za-z]", word) and re.search(r"\d", word))


def _normalize_for_lookup(word: str) -> str:
    base = re.sub(r"['’]s$", "", word, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]", "", base)
    return base.lower()

# ============================================================
# WORD PROCESSORS
# ============================================================

def _process_hyphenated(word: str, cfg: dict) -> str:
    parts = word.split("-")
    processed = []

    for part in parts:
        lookup = _normalize_for_lookup(part)

        if _ROMAN_RE.match(part):
            processed.append(part.upper())
        elif lookup in cfg["_always_uppercase_norm"]:
            processed.append(part.upper())
        elif _contains_letters_and_digits(part):
            processed.append(re.sub(r"[A-Za-z]+", lambda m: m.group(0).upper(), part))
        elif lookup in cfg["_small_words_norm"]:
            processed.append(part.lower())
        else:
            processed.append(part[:1].upper() + part[1:].lower())

    return "-".join(processed)


def _process_word(word: str, cfg: dict) -> str:
    if not word:
        return word

    lower = word.lower()

    # Force "Ft." normalization
    if lower in {"ft", "ft."}:
        return "Ft."

    # Exact overrides
    if lower in cfg["_exact_map"]:
        return cfg["_exact_map"][lower]

    # Roman numerals
    if _ROMAN_RE.match(word):
        return word.upper()

    # Alphanumeric tokens
    if "-" not in word and _contains_letters_and_digits(word):
        return re.sub(r"[A-Za-z]+", lambda m: m.group(0).upper(), word)

    # Hyphenated
    if "-" in word:
        return _process_hyphenated(word, cfg)

    # Possessives
    m = re.match(r"^([A-Za-z]+)(['’]s)$", word)
    if m:
        base, poss = m.groups()
        lookup = _normalize_for_lookup(base)

        if lookup in cfg["_always_uppercase_norm"]:
            return base.upper() + poss.lower()
        if lookup in cfg["_small_words_norm"]:
            return base.lower() + poss.lower()

        return base[:1].upper() + base[1:] + poss

    # Always uppercase
    if _normalize_for_lookup(word) in cfg["_always_uppercase_norm"]:
        return word.upper()

    # Small words
    if _normalize_for_lookup(word) in cfg["_small_words_norm"]:
        return word.lower()

    return word[:1].upper() + word[1:].lower()

# ============================================================
# PUBLIC API
# ============================================================

def to_title_case(text: str) -> str:
    """
    Convert a string into properly formatted title case.
    """
    if not text:
        return ""

    cfg = _CFG

    # Process parentheses first
    def paren_repl(match):
        inner = match.group(1)
        return f"({ ' '.join(_process_word(w, cfg) for w in inner.split()) })"

    text = re.sub(r"\(([^)]+)\)", paren_repl, text)

    words = text.split()
    if not words:
        return ""

    # Ordinal handling for first word
    ordinal_match = _ORDINAL_RE.match(words[0])
    if ordinal_match:
        num, suf = ordinal_match.groups()
        words[0] = f"{num}{suf.lower()}"
    else:
        words[0] = _process_word(words[0], cfg)

    # Remaining words
    for i in range(1, len(words)):
        words[i] = _process_word(words[i], cfg)

    return " ".join(words)


def format_title(raw_title: str) -> str:
    """
    Clean and format a raw title string.
    """
    return to_title_case(remove_invalid_chars(raw_title or ""))
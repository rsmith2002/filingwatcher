"""
config.py — central configuration for CeoWatcher.

Add tickers to COMPANIES to expand coverage; everything else auto-adapts.
Set EDGAR_IDENTITY via the EDGAR_IDENTITY environment variable on Render,
or drop it in a local .env file (loaded automatically by python-dotenv).
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Identity (required by SEC for edgartools API access)
# ---------------------------------------------------------------------------
EDGAR_IDENTITY: str = os.environ.get("EDGAR_IDENTITY", "r63848@proton.me")

# ---------------------------------------------------------------------------
# Company watchlist  — add rows here to expand coverage
# ---------------------------------------------------------------------------
COMPANIES: list[tuple[str, str]] = [
    ("NVDA",  "Nvidia"),
    ("AAPL",  "Apple Inc."),
    ("MSFT",  "Microsoft"),
    ("AMZN",  "Amazon"),
    ("GOOGL", "Alphabet Inc."),
    ("META",  "Meta Platforms"),
    ("TSLA",  "Tesla"),
    ("AVGO",  "Broadcom"),
    ("BRK-B", "Berkshire Hathaway"),
]

# ---------------------------------------------------------------------------
# Historical backfill
# ---------------------------------------------------------------------------
BACKFILL_START: str = "2015-01-01"   # 10 years of Form 4 history

# ---------------------------------------------------------------------------
# Return-window definitions  (label → days)
# Used in analytics pre-computation and dashboard filters
# ---------------------------------------------------------------------------
RETURN_WINDOWS: dict[str, int] = {
    "2w":  14,
    "1m":  30,
    "3m":  90,
    "6m":  180,
    "1y":  365,
    "2y":  730,
    "3y":  1095,
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
# Render injects DATABASE_URL automatically.
# For local dev: set it in .env as DATABASE_URL=postgresql://user:pass@host/db
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://ceowatcher:ceowatcher@localhost:5432/ceowatcher",
)
# SQLAlchemy requires postgresql:// not postgres://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

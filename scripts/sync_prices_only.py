"""
scripts/sync_prices_only.py

Sync / refresh price history for all tickers (or a subset).
Safe to run at any time — only fetches days not already in the DB.

Usage:
    python scripts/sync_prices_only.py
    python scripts/sync_prices_only.py --ticker NVDA AAPL
    python scripts/sync_prices_only.py --start 2015-01-01
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import COMPANIES
from db.session import init_db
from ingestion.prices import sync_prices

parser = argparse.ArgumentParser(description="Sync price history")
parser.add_argument("--ticker", nargs="+", help="Limit to specific tickers")
parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: 10 years ago)")
args = parser.parse_args()

init_db()

tickers = args.ticker if args.ticker else [t for t, _ in COMPANIES]
print(f"Syncing prices for: {', '.join(tickers)}")

n = sync_prices(tickers=tickers, start_date=args.start, verbose=True)
print(f"\nDone — {n} new rows inserted")

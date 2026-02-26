"""
scripts/backfill.py

One-time historical backfill — runs through all companies year by year
going back to BACKFILL_START (default 2015-01-01).

Run once after deploying:
    python scripts/backfill.py

Options:
    --start YYYY-MM-DD   Override start date (default: config.BACKFILL_START)
    --ticker NVDA        Only backfill one ticker
    --skip-prices        Skip price history download (if already done)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import BACKFILL_START, COMPANIES
from db.session import init_db
from ingestion.analytics import refresh_all_analytics
from ingestion.fetchers import upsert_large_holder_stakes, upsert_section16
from ingestion.prices import sync_prices


def _seed_companies(companies: list[tuple[str, str]]) -> None:
    """Insert company rows so foreign keys resolve before any data is written."""
    from db.models import Company
    from db.session import get_session
    from ingestion.fetchers import _resolve_company

    session = get_session()
    try:
        for ticker, display_name in companies:
            if session.get(Company, ticker):
                continue
            ec = _resolve_company(ticker, verbose=False)
            session.merge(Company(
                ticker=ticker,
                name=ec.name if ec else display_name,
                cik=str(ec.cik) if ec else None,
            ))
        session.commit()
        print(f"  Companies seeded: {[t for t, _ in companies]}")
    finally:
        session.close()


def run_backfill(
    start_date: str = BACKFILL_START,
    tickers_filter: list[str] | None = None,
    skip_prices: bool = False,
    verbose: bool = True,
):
    init_db()
    end_date = str(date.today())

    companies = COMPANIES
    if tickers_filter:
        companies = [(t, n) for t, n in COMPANIES if t in tickers_filter]
        if not companies:
            print(f"No matching companies for tickers: {tickers_filter}")
            sys.exit(1)

    tickers = [t for t, _ in companies]

    print(f"\n{'='*60}")
    print(f"CeoWatcher BACKFILL")
    print(f"  Date range : {start_date} → {end_date}")
    print(f"  Tickers    : {', '.join(tickers)}")
    print(f"{'='*60}\n")

    # ── Seed companies first (required for FK constraints) ────────────────
    print("[0/4] Seeding companies table …")
    _seed_companies(companies)

    # ── Prices ────────────────────────────────────────────────────────────
    if not skip_prices:
        print("[1/4] Downloading full price history …")
        sync_prices(tickers=tickers, start_date=start_date, verbose=verbose)
    else:
        print("[1/4] Skipping price download (--skip-prices)")

    # ── Section 16 — iterate year by year to avoid timeouts ───────────────
    print("\n[2/4] Fetching Section 16 filings year by year …")
    from datetime import datetime
    start_yr = int(start_date[:4])
    end_yr   = int(end_date[:4])
    total_s16 = 0

    for year in range(start_yr, end_yr + 1):
        yr_start = f"{year}-01-01"
        yr_end   = f"{year}-12-31" if year < end_yr else end_date
        print(f"\n  Year {year}: {yr_start} → {yr_end}")
        try:
            n = upsert_section16(
                companies=companies,
                start_date=yr_start,
                end_date=yr_end,
                verbose=verbose,
            )
            total_s16 += n
            print(f"  Year {year}: {n} new rows")
        except Exception as exc:
            print(f"  ERROR in year {year}: {exc}")

    print(f"\n  Total Section 16 rows inserted: {total_s16:,}")

    # ── Large-holder stakes ───────────────────────────────────────────────
    print("\n[3/4] Fetching large-holder stakes (13D/13G) …")
    try:
        n_stakes = upsert_large_holder_stakes(
            companies=companies,
            start_date=start_date,
            end_date=end_date,
            verbose=verbose,
        )
        print(f"  Stakes rows inserted: {n_stakes:,}")
    except Exception as exc:
        print(f"  ERROR in stakes fetch: {exc}")

    # ── Analytics ─────────────────────────────────────────────────────────
    print("\n[4/4] Computing analytics for all insiders …")
    n_analytics = refresh_all_analytics(verbose=verbose)
    print(f"  Analytics records: {n_analytics:,}")

    print(f"\n{'='*60}")
    print("Backfill complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CeoWatcher historical backfill")
    parser.add_argument("--start", default=BACKFILL_START,
                        help=f"Start date (default: {BACKFILL_START})")
    parser.add_argument("--ticker", nargs="+",
                        help="Only process specific tickers (e.g. --ticker NVDA AAPL)")
    parser.add_argument("--skip-prices", action="store_true",
                        help="Skip price history download")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    run_backfill(
        start_date=args.start,
        tickers_filter=args.ticker,
        skip_prices=args.skip_prices,
        verbose=not args.quiet,
    )

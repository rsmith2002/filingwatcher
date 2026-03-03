"""
ingestion/prices.py

Syncs daily adjusted close prices for all tracked tickers into price_history.
Uses batch yfinance downloads to avoid per-ticker sequential HTTP requests.
"""

from collections import defaultdict
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import COMPANIES
from db.models import PriceHistory
from db.session import get_session

# Maximum tickers per yfinance batch request — 100 is reliable
_BATCH_SIZE = 100


def sync_prices(
    tickers: list[str] | None = None,
    start_date: str | None = None,
    verbose: bool = True,
) -> int:
    """
    Download price history for tickers and upsert into price_history.

    tickers    — defaults to all COMPANIES tickers
    start_date — defaults to 10 years ago; on subsequent runs only fetches new days

    Returns count of new rows inserted.

    Performance: uses a single SQL GROUP BY to find each ticker's latest date,
    then groups tickers by start date and issues batch yfinance downloads.
    For a typical incremental run all tickers share the same start date, so
    the entire sync completes in 1-3 batch requests instead of 494 individual ones.
    """
    if tickers is None:
        tickers = [t for t, _ in COMPANIES]

    # ── 1. Find latest stored date per ticker in one query ────────────────────
    session = get_session()
    try:
        rows = (
            session.query(PriceHistory.ticker, func.max(PriceHistory.date))
            .filter(PriceHistory.ticker.in_(tickers))
            .group_by(PriceHistory.ticker)
            .all()
        )
    finally:
        session.close()

    latest_by_ticker: dict[str, date] = {t: d for t, d in rows}

    default_start = start_date or str(date.today() - timedelta(days=365 * 10))
    fetch_to = str(date.today() + timedelta(days=1))

    # ── 2. Group tickers by their fetch-from date ─────────────────────────────
    by_start: dict[str, list[str]] = defaultdict(list)
    for ticker in tickers:
        latest = latest_by_ticker.get(ticker)
        if latest:
            fetch_from = str(latest + timedelta(days=1))
        else:
            fetch_from = default_start

        if fetch_from <= fetch_to:
            by_start[fetch_from].append(ticker)

    if verbose:
        total_to_fetch = sum(len(v) for v in by_start.values())
        skipped = len(tickers) - total_to_fetch
        print(
            f"  {skipped} tickers up to date; "
            f"{total_to_fetch} to fetch in {len(by_start)} date group(s)"
        )

    # ── 3. Batch download per date group ──────────────────────────────────────
    new_rows = 0

    for fetch_from, group_tickers in sorted(by_start.items()):
        for i in range(0, len(group_tickers), _BATCH_SIZE):
            batch = group_tickers[i : i + _BATCH_SIZE]
            if verbose:
                print(
                    f"  Batch {len(batch)} tickers from {fetch_from} "
                    f"(batch {i // _BATCH_SIZE + 1})"
                )

            try:
                raw = yf.download(
                    batch,
                    start=fetch_from,
                    end=fetch_to,
                    auto_adjust=True,
                    progress=False,
                )
            except Exception as exc:
                print(f"  WARN batch download ({fetch_from}): {exc}")
                continue

            if raw.empty:
                continue

            # Extract a DataFrame of closes: index=date, columns=tickers
            if isinstance(raw.columns, pd.MultiIndex):
                level0 = raw.columns.get_level_values(0)
                if "Close" not in level0:
                    continue
                closes_df = raw["Close"]  # DataFrame or Series
                if isinstance(closes_df, pd.Series):
                    closes_df = closes_df.to_frame(name=batch[0])
            else:
                # Single ticker batch — flat columns
                if "Close" not in raw.columns:
                    continue
                closes_df = raw[["Close"]].rename(columns={"Close": batch[0]})

            # ── 4. Upsert to DB (short-lived session per batch) ───────────────
            session = get_session()
            try:
                for ticker in batch:
                    if ticker not in closes_df.columns:
                        continue
                    for dt, price_val in closes_df[ticker].items():
                        if pd.isna(price_val):
                            continue
                        stmt = (
                            pg_insert(PriceHistory.__table__)
                            .values(
                                ticker=ticker,
                                date=dt.date() if hasattr(dt, "date") else dt,
                                close=float(price_val),
                            )
                            .on_conflict_do_update(
                                index_elements=["ticker", "date"],
                                set_={"close": float(price_val)},
                            )
                        )
                        session.execute(stmt)
                        new_rows += 1
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    if verbose:
        print(f"  Price sync complete: {new_rows} new rows")
    return new_rows


def get_price_series(ticker: str) -> pd.Series:
    """Return a date-indexed price series for a ticker from the DB."""
    session = get_session()
    try:
        rows = (
            session.query(PriceHistory.date, PriceHistory.close)
            .filter_by(ticker=ticker)
            .order_by(PriceHistory.date)
            .all()
        )
        if not rows:
            return pd.Series(dtype=float)
        dates, closes = zip(*rows)
        return pd.Series(closes, index=pd.to_datetime(dates))
    finally:
        session.close()


def get_price_on_or_after(ticker: str, target_date) -> float | None:
    """Closest closing price on or after target_date for the given ticker."""
    session = get_session()
    try:
        if isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).date()
        row = (
            session.query(PriceHistory.close)
            .filter(PriceHistory.ticker == ticker, PriceHistory.date >= target_date)
            .order_by(PriceHistory.date)
            .first()
        )
        return float(row[0]) if row else None
    finally:
        session.close()


def get_latest_price(ticker: str) -> float | None:
    """Most recent closing price in the DB for ticker."""
    session = get_session()
    try:
        row = (
            session.query(PriceHistory.close)
            .filter_by(ticker=ticker)
            .order_by(PriceHistory.date.desc())
            .first()
        )
        return float(row[0]) if row else None
    finally:
        session.close()

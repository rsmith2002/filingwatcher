"""
ingestion/prices.py

Syncs daily adjusted close prices for all tracked tickers into price_history.
"""

from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from config import COMPANIES
from db.models import PriceHistory
from db.session import get_session


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
    """
    if tickers is None:
        tickers = [t for t, _ in COMPANIES]

    session = get_session()
    new_rows = 0

    try:
        for ticker in tickers:
            # Find the latest date we already have for this ticker
            latest = (
                session.query(PriceHistory.date)
                .filter_by(ticker=ticker)
                .order_by(PriceHistory.date.desc())
                .first()
            )
            if latest:
                fetch_from = str(latest[0] + timedelta(days=1))
            elif start_date:
                fetch_from = start_date
            else:
                fetch_from = str(date.today() - timedelta(days=365 * 10))

            fetch_to = str(date.today() + timedelta(days=1))

            if fetch_from > fetch_to:
                if verbose:
                    print(f"  {ticker}: prices up to date")
                continue

            if verbose:
                print(f"  {ticker}: fetching prices {fetch_from} → {date.today()}")

            try:
                raw = yf.download(
                    ticker,
                    start=fetch_from,
                    end=fetch_to,
                    auto_adjust=True,
                    progress=False,
                )
            except Exception as exc:
                print(f"  WARN price fetch {ticker}: {exc}")
                continue

            if raw.empty:
                continue

            # Handle both single and multi-ticker yfinance output
            if isinstance(raw.columns, pd.MultiIndex):
                closes = raw.xs("Close", axis=1, level=0) if "Close" in raw.columns.get_level_values(0) else raw["Close"]
            else:
                closes = raw["Close"]

            if isinstance(closes, pd.Series):
                closes = closes.to_frame(name=ticker)

            for dt, row in closes.iterrows():
                price_val = row.iloc[0] if hasattr(row, "iloc") else float(row)
                if pd.isna(price_val):
                    continue
                obj = PriceHistory(
                    ticker=ticker,
                    date=dt.date() if hasattr(dt, "date") else dt,
                    close=float(price_val),
                )
                session.merge(obj)
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

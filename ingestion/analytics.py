"""
ingestion/analytics.py

Computes per-insider analytics from the database and writes to insider_analytics.
Called after each ingestion run to keep the table fresh.
"""

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from config import RETURN_WINDOWS
from db.models import InsiderAnalytics, PriceHistory, Section16Filing
from db.session import get_session
from ingestion.prices import get_price_on_or_after, get_latest_price


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_sum(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    return float(s.sum()) if not s.empty else None


def _wacb(shares: pd.Series, prices: pd.Series) -> Optional[float]:
    mask = shares.notna() & prices.notna()
    s, p = shares[mask], prices[mask]
    if s.empty or s.sum() == 0:
        return None
    return float((s * p).sum() / s.sum())


def _price_pct(price_a: Optional[float], price_b: Optional[float]) -> Optional[float]:
    if price_a and price_b and price_a > 0:
        return (price_b - price_a) / price_a * 100
    return None


def _window_price(ticker: str, base_date: date, days: int) -> Optional[float]:
    """Return closing price ~N days after base_date (or nearest available)."""
    target = base_date + timedelta(days=days)
    return get_price_on_or_after(ticker, target)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _compute_for_insider(
    ticker: str,
    insider_name: str,
    rows: pd.DataFrame,
) -> dict:
    """Compute analytics dict for one (ticker, insider_name) pair."""
    nd = rows[rows["is_derivative"] == False].copy()
    nd = nd.sort_values("transaction_date")

    latest_row   = rows.sort_values("filing_date").iloc[-1]
    officer_title = latest_row.get("officer_title")
    is_director   = bool(latest_row.get("is_director"))
    is_officer    = bool(latest_row.get("is_officer"))
    is_ten_pct    = bool(latest_row.get("is_ten_pct_owner"))
    company_name  = latest_row.get("company_name")

    dated = nd[nd["transaction_date"].notna()]
    first_txn_dt  = dated["transaction_date"].min() if not dated.empty else None
    last_filing   = rows["filing_date"].max()

    # Prices
    cur_price     = get_latest_price(ticker)
    entry_price   = get_price_on_or_after(ticker, first_txn_dt) if first_txn_dt else None
    stock_pct     = _price_pct(entry_price, cur_price)

    # Return windows
    window_pcts = {}
    for label, days in RETURN_WINDOWS.items():
        if first_txn_dt:
            w_price = _window_price(ticker, first_txn_dt, days)
            window_pcts[f"pct_{label}"] = _price_pct(entry_price, w_price)
        else:
            window_pcts[f"pct_{label}"] = None

    # Position
    pos_rows     = nd[nd["shares_remaining"].notna()].sort_values("transaction_date")
    last_shares  = float(pos_rows["shares_remaining"].iloc[-1]) if not pos_rows.empty else None
    pos_value    = (last_shares * cur_price) if (last_shares and cur_price) else None

    # Open-market purchases (Code=P)
    buys         = nd[nd["transaction_code"] == "P"]
    n_buys       = len(buys)
    buy_shares   = _safe_sum(buys["shares"])
    buy_cost     = _safe_sum(buys["value"])
    buy_wacb     = _wacb(buys["shares"], buys["price"])
    if buy_wacb and cur_price and buy_shares:
        buy_unrlzd_pct = _price_pct(buy_wacb, cur_price)
        buy_unrlzd_usd = (cur_price - buy_wacb) * buy_shares
    else:
        buy_unrlzd_pct = buy_unrlzd_usd = None

    # Open-market sales (Code=S)
    sells        = nd[nd["transaction_code"] == "S"]
    n_sells      = len(sells)
    sell_shares  = _safe_sum(sells["shares"])
    sell_procs   = _safe_sum(sells["value"])
    sell_wacb    = _wacb(sells["shares"], sells["price"])
    realized_pct = _price_pct(buy_wacb, sell_wacb) if (buy_wacb and sell_wacb) else None

    # Awards
    awards        = nd[nd["transaction_code"] == "A"]
    award_shares  = _safe_sum(awards["shares"])
    award_cur_val = (award_shares * cur_price) if (award_shares and cur_price) else None

    # Net conviction
    net_open_mkt = (buy_shares or 0) - (sell_shares or 0)

    # 10b5-1 plan
    plan_trades  = nd[nd["is_10b5_1_plan"] == True]
    pct_plan     = len(plan_trades) / len(nd) * 100 if len(nd) else 0.0

    return dict(
        ticker=ticker,
        insider_name=insider_name,
        insider_cik=latest_row.get("insider_cik"),
        company_name=company_name,
        officer_title=officer_title,
        is_director=is_director,
        is_officer=is_officer,
        is_ten_pct_owner=is_ten_pct,
        first_txn_date=first_txn_dt,
        last_filing_date=last_filing,
        entry_price=entry_price,
        current_price=cur_price,
        stock_pct_since_entry=stock_pct,
        **window_pcts,
        last_reported_shares=last_shares,
        current_position_value=pos_value,
        n_open_mkt_buys=n_buys,
        open_mkt_shares_bought=buy_shares,
        open_mkt_total_cost=buy_cost,
        open_mkt_wacb=buy_wacb,
        open_mkt_unrealized_pct=buy_unrlzd_pct,
        open_mkt_unrealized_usd=buy_unrlzd_usd,
        n_open_mkt_sells=n_sells,
        open_mkt_shares_sold=sell_shares,
        open_mkt_total_proceeds=sell_procs,
        open_mkt_avg_sell_price=sell_wacb,
        realized_pct=realized_pct,
        shares_awarded=award_shares,
        award_current_value=award_cur_val,
        net_open_mkt_shares=net_open_mkt,
        pct_trades_on_10b5_plan=pct_plan,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def refresh_analytics_for_ticker(ticker: str, verbose: bool = False) -> int:
    """
    Recompute analytics for all insiders of a given ticker.
    Returns the count of records upserted.
    """
    session = get_session()
    count = 0
    try:
        rows_q = (
            session.query(Section16Filing)
            .filter_by(ticker=ticker)
            .all()
        )
        if not rows_q:
            return 0

        df = pd.DataFrame([{
            "ticker":           r.ticker,
            "insider_name":     r.insider_name,
            "insider_cik":      r.insider_cik,
            "company_name":     r.company_name,
            "officer_title":    r.officer_title,
            "is_director":      r.is_director,
            "is_officer":       r.is_officer,
            "is_ten_pct_owner": r.is_ten_pct_owner,
            "filing_date":      r.filing_date,
            "transaction_date": r.transaction_date,
            "transaction_code": r.transaction_code,
            "acquired_disposed": r.acquired_disposed,
            "shares":           r.shares,
            "price":            r.price,
            "value":            r.value,
            "shares_remaining": r.shares_remaining,
            "is_derivative":    r.is_derivative,
            "is_10b5_1_plan":   r.is_10b5_1_plan,
        } for r in rows_q])

        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
        df["filing_date"]      = pd.to_datetime(df["filing_date"],      errors="coerce").dt.date

        for insider_name, grp in df.groupby("insider_name", dropna=False):
            if pd.isna(insider_name) or insider_name is None:
                continue
            analytics_dict = _compute_for_insider(ticker, insider_name, grp.copy())
            obj = session.query(InsiderAnalytics).filter_by(
                ticker=ticker, insider_name=insider_name
            ).first()
            if obj:
                for k, v in analytics_dict.items():
                    setattr(obj, k, v)
                from datetime import datetime
                obj.computed_at = datetime.utcnow()
            else:
                obj = InsiderAnalytics(**analytics_dict)
                session.add(obj)
            count += 1

        session.commit()
        if verbose:
            print(f"  Analytics refreshed for {ticker}: {count} insiders")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return count


def refresh_all_analytics(verbose: bool = True) -> int:
    """Refresh analytics for every ticker that has Section 16 data."""
    from config import COMPANIES
    total = 0
    for ticker, _ in COMPANIES:
        n = refresh_analytics_for_ticker(ticker, verbose)
        total += n
    return total

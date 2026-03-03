"""
ingestion/backtest.py

Portfolio simulation engine for CeoWatcher.

Strategy:
  - BUY  : enter next trading-day after a flag's filing_date (public info date)
  - SIZE : starting_capital × base_pct × severity_weight
           HIGH=3×  MEDIUM=2×  LOW=1×
  - EXIT : whichever comes first —
           (a) same insider files a Code=S/D non-derivative sell →
               sell proportionally (sell_shares / original_buy_shares, capped 1.0)
           (b) max_holding_days elapsed  → full exit
           (c) price drops > stop_loss_pct from entry  → full exit
  - SLIPPAGE: applied symmetrically on entry (+) and exit (-)
              Default 0.10% (10 bps) — realistic for T212 on S&P 500 stocks

No look-ahead bias: entry price uses the first close available AFTER filing_date.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

from db.models import Flag, PriceHistory, Section16Filing
from db.session import get_session

SEVERITY_WEIGHTS: dict[str, int] = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
    trades_df: pd.DataFrame
    equity_series: pd.Series    # indexed by date, values = portfolio USD
    spy_series: pd.Series       # SPY normalised to starting_capital
    stats: dict
    params: dict


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_backtest_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all data needed for backtesting.

    Returns
    -------
    flags_df  : one row per flagged buy (Flag joined to Section16Filing)
    sells_df  : all Code=S/D non-derivative filings (for exit signals)
    prices_df : full price_history for all flagged tickers
    """
    session = get_session()
    try:
        # ── 1. Flagged buys ───────────────────────────────────────────────
        flag_rows = (
            session.query(Flag, Section16Filing)
            .join(Section16Filing, Flag.accession_no == Section16Filing.accession_no)
            .filter(
                Flag.is_dismissed == False,
                Section16Filing.transaction_code == "P",
                Section16Filing.is_derivative == False,
                Section16Filing.shares.isnot(None),
                Section16Filing.filing_date.isnot(None),
            )
            .all()
        )

        if not flag_rows:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        flags_records = [
            {
                "flag_id":        flag.id,
                "flag_type":      flag.flag_type,
                "severity":       flag.severity,
                "ticker":         flag.ticker,
                "insider_name":   flag.insider_name,
                "filing_date":    filing.filing_date,
                "transaction_date": filing.transaction_date,
                "buy_price":      filing.price,
                "buy_shares":     float(filing.shares or 0),
                "description":    flag.description,
            }
            for flag, filing in flag_rows
        ]
        flags_df = pd.DataFrame(flags_records)
        flags_df["filing_date"] = pd.to_datetime(flags_df["filing_date"])
        # Remove rows with zero shares (can't compute proportional sell)
        flags_df = flags_df[flags_df["buy_shares"] > 0].copy()

        # ── 2. Sell signals (Code=S or D, non-derivative) ─────────────────
        sell_rows = (
            session.query(Section16Filing)
            .filter(
                Section16Filing.transaction_code.in_(["S", "D"]),
                Section16Filing.is_derivative == False,
                Section16Filing.shares.isnot(None),
                Section16Filing.filing_date.isnot(None),
            )
            .all()
        )

        if sell_rows:
            sells_df = pd.DataFrame([
                {
                    "sell_id":      row.id,
                    "ticker":       row.ticker,
                    "insider_name": row.insider_name,
                    "filing_date":  row.filing_date,
                    "shares":       float(row.shares or 0),
                }
                for row in sell_rows
            ])
            sells_df["filing_date"] = pd.to_datetime(sells_df["filing_date"])
            sells_df = sells_df[sells_df["shares"] > 0].sort_values("filing_date")
        else:
            sells_df = pd.DataFrame()

        # ── 3. Price history for flagged tickers ──────────────────────────
        relevant_tickers = flags_df["ticker"].unique().tolist()
        price_rows = (
            session.query(PriceHistory)
            .filter(PriceHistory.ticker.in_(relevant_tickers))
            .order_by(PriceHistory.ticker, PriceHistory.date)
            .all()
        )

        if price_rows:
            prices_df = pd.DataFrame([
                {"ticker": p.ticker, "date": p.date, "close": p.close}
                for p in price_rows
            ])
            prices_df["date"] = pd.to_datetime(prices_df["date"])
        else:
            prices_df = pd.DataFrame()

        return flags_df, sells_df, prices_df

    finally:
        session.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_price_index(prices_df: pd.DataFrame) -> dict[str, dict[date, float]]:
    """Build {ticker: {date: close}} for O(1) lookups."""
    idx: dict[str, dict[date, float]] = {}
    for ticker, grp in prices_df.groupby("ticker"):
        idx[ticker] = {row.date() if hasattr(row, "date") else row: close
                       for row, close in zip(grp["date"], grp["close"])}
    return idx


def _next_available_price(
    price_index: dict[str, dict[date, float]],
    ticker: str,
    from_date: date,
) -> tuple[date | None, float | None]:
    """Return (date, close) for the first trading day on-or-after from_date."""
    ticker_prices = price_index.get(ticker, {})
    for d in sorted(ticker_prices.keys()):
        if d >= from_date:
            return d, ticker_prices[d]
    return None, None


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def run_backtest(
    starting_capital: float = 100_000.0,
    base_pct: float = 0.05,
    max_holding_days: int = 90,
    stop_loss_pct: float = 0.10,
    slippage_pct: float = 0.001,
    risk_free_rate: float = 0.05,
) -> BacktestResult | None:
    """
    Execute the portfolio simulation and return a BacktestResult.
    Returns None if there is insufficient data (no flags or no prices).
    """
    flags_df, sells_df, prices_df = load_backtest_data()

    if flags_df.empty or prices_df.empty:
        return None

    price_index = _build_price_index(prices_df)

    # ── Pre-compute entry schedule ────────────────────────────────────────
    # Entry = first trading day AFTER filing_date (no look-ahead)
    entries: list[dict] = []
    for _, flag in flags_df.iterrows():
        fdate = flag["filing_date"]
        filing_day = fdate.date() if hasattr(fdate, "date") else fdate
        target = filing_day + timedelta(days=1)
        entry_date, entry_price_raw = _next_available_price(price_index, flag["ticker"], target)
        if entry_date is None:
            continue
        entries.append({**flag.to_dict(), "entry_date": entry_date,
                         "entry_price_raw": entry_price_raw})

    if not entries:
        return None

    entries_by_date: dict[date, list[dict]] = {}
    for e in entries:
        entries_by_date.setdefault(e["entry_date"], []).append(e)

    # ── Pre-group sell signals ────────────────────────────────────────────
    sell_signals: dict[tuple[str, str], list[dict]] = {}
    if not sells_df.empty:
        for _, row in sells_df.iterrows():
            key = (row["ticker"], row["insider_name"])
            sell_signals.setdefault(key, []).append({
                "sell_id":    row["sell_id"],
                "filing_date": row["filing_date"].date() if hasattr(row["filing_date"], "date") else row["filing_date"],
                "shares":     row["shares"],
            })
        for key in sell_signals:
            sell_signals[key].sort(key=lambda x: x["filing_date"])

    # ── Trading days (sorted) ─────────────────────────────────────────────
    all_trading_days: list[date] = sorted(
        {d.date() if hasattr(d, "date") else d for d in prices_df["date"]}
    )
    first_entry = min(entries_by_date.keys())
    sim_days = [d for d in all_trading_days if d >= first_entry]

    # ── Simulation state ──────────────────────────────────────────────────
    cash = starting_capital
    open_positions: dict[int, dict] = {}   # flag_id → position dict
    completed_trades: list[dict] = []
    daily_equity: dict[date, float] = {}
    last_known_price: dict[str, float] = {}

    for day in sim_days:
        # ── 1. Open new positions ─────────────────────────────────────────
        for entry in entries_by_date.get(day, []):
            weight = SEVERITY_WEIGHTS.get(entry["severity"], 1)
            size_usd = starting_capital * base_pct * weight

            if cash < size_usd * 0.50:
                continue  # skip if very low on cash

            entry_price = entry["entry_price_raw"] * (1.0 + slippage_pct)
            shares_held = size_usd / entry_price
            cash -= size_usd

            open_positions[entry["flag_id"]] = {
                "flag_id":          entry["flag_id"],
                "flag_type":        entry["flag_type"],
                "severity":         entry["severity"],
                "ticker":           entry["ticker"],
                "insider_name":     entry["insider_name"],
                "shares":           shares_held,
                "entry_price":      entry_price,
                "entry_price_raw":  entry["entry_price_raw"],
                "entry_date":       day,
                "cost":             size_usd,
                "insider_buy_shares": entry["buy_shares"],  # denominator for sell %
                "sell_idx":         0,
            }

        # ── 2. Update last known prices ───────────────────────────────────
        for ticker, ticker_prices in price_index.items():
            if day in ticker_prices:
                last_known_price[ticker] = ticker_prices[day]

        # ── 3. Check exits ────────────────────────────────────────────────
        to_close: list[tuple] = []  # (flag_id, reason, fraction, exit_price)

        for fid, pos in open_positions.items():
            current_price = price_index.get(pos["ticker"], {}).get(day)
            if current_price is None:
                continue  # no price today — skip exit checks

            holding_days = (day - pos["entry_date"]).days
            exit_reason: str | None = None
            exit_fraction = 1.0

            # Stop loss (full exit)
            if current_price <= pos["entry_price_raw"] * (1.0 - stop_loss_pct):
                exit_reason = "STOP_LOSS"

            # Max holding period (full exit)
            elif holding_days >= max_holding_days:
                exit_reason = "MAX_HOLD"

            # Insider sell signal (proportional exit)
            else:
                sell_list = sell_signals.get((pos["ticker"], pos["insider_name"]), [])
                idx = pos["sell_idx"]
                while idx < len(sell_list):
                    sell = sell_list[idx]
                    if sell["filing_date"] <= day:
                        i_shares = pos["insider_buy_shares"]
                        if i_shares > 0 and sell["shares"] > 0:
                            proportion = min(sell["shares"] / i_shares, 1.0)
                            exit_reason = "INSIDER_SELL"
                            exit_fraction = proportion
                            pos["sell_idx"] = idx + 1
                        else:
                            pos["sell_idx"] = idx + 1
                        break
                    break  # future sell — stop looking

            if exit_reason:
                exit_price = current_price * (1.0 - slippage_pct)
                to_close.append((fid, exit_reason, exit_fraction, exit_price))

        # ── 4. Process exits ──────────────────────────────────────────────
        for fid, exit_reason, exit_fraction, exit_price in to_close:
            pos = open_positions[fid]
            is_partial = (exit_reason == "INSIDER_SELL" and exit_fraction < 1.0)

            sold_shares = pos["shares"] * exit_fraction
            cost_basis  = pos["cost"] * exit_fraction
            proceeds    = sold_shares * exit_price
            cash       += proceeds
            return_pct  = (exit_price / pos["entry_price"] - 1) * 100
            return_usd  = proceeds - cost_basis

            completed_trades.append({
                "ticker":        pos["ticker"],
                "insider_name":  pos["insider_name"],
                "flag_type":     pos["flag_type"],
                "severity":      pos["severity"],
                "entry_date":    pos["entry_date"],
                "exit_date":     day,
                "holding_days":  (day - pos["entry_date"]).days,
                "exit_reason":   exit_reason,
                "entry_price":   round(pos["entry_price"], 4),
                "exit_price":    round(exit_price, 4),
                "shares":        round(sold_shares, 4),
                "position_usd":  round(cost_basis, 2),
                "return_pct":    round(return_pct, 2),
                "return_usd":    round(return_usd, 2),
            })

            if is_partial:
                # Reduce remaining position
                open_positions[fid]["shares"]            -= sold_shares
                open_positions[fid]["cost"]              -= cost_basis
                open_positions[fid]["insider_buy_shares"] = max(
                    pos["insider_buy_shares"] * (1.0 - exit_fraction), 1e-6
                )
                if open_positions[fid]["shares"] < 1e-4:
                    del open_positions[fid]
            else:
                del open_positions[fid]

        # ── 5. Mark-to-market ─────────────────────────────────────────────
        pos_value = sum(
            pos["shares"] * last_known_price.get(pos["ticker"], pos["entry_price_raw"])
            for pos in open_positions.values()
        )
        daily_equity[day] = cash + pos_value

    # ── Close remaining positions at last available price ─────────────────
    last_day = sim_days[-1] if sim_days else date.today()
    for fid, pos in list(open_positions.items()):
        lp = last_known_price.get(pos["ticker"], pos["entry_price_raw"])
        exit_price = lp * (1.0 - slippage_pct)
        proceeds   = pos["shares"] * exit_price
        cash      += proceeds
        completed_trades.append({
            "ticker":       pos["ticker"],
            "insider_name": pos["insider_name"],
            "flag_type":    pos["flag_type"],
            "severity":     pos["severity"],
            "entry_date":   pos["entry_date"],
            "exit_date":    last_day,
            "holding_days": (last_day - pos["entry_date"]).days,
            "exit_reason":  "OPEN",
            "entry_price":  round(pos["entry_price"], 4),
            "exit_price":   round(exit_price, 4),
            "shares":       round(pos["shares"], 4),
            "position_usd": round(pos["cost"], 2),
            "return_pct":   round((exit_price / pos["entry_price"] - 1) * 100, 2),
            "return_usd":   round(proceeds - pos["cost"], 2),
        })

    # ── Build result objects ──────────────────────────────────────────────
    equity_series = pd.Series(daily_equity).sort_index()
    trades_df     = pd.DataFrame(completed_trades) if completed_trades else pd.DataFrame()

    # ── SPY benchmark (fetched live) ──────────────────────────────────────
    spy_series = pd.Series(dtype=float)
    try:
        spy_start = str(sim_days[0] - timedelta(days=5)) if sim_days else "2020-01-01"
        spy_raw = yf.download(
            "SPY", start=spy_start,
            end=str(date.today() + timedelta(days=1)),
            auto_adjust=True, progress=False,
        )
        if not spy_raw.empty:
            if isinstance(spy_raw.columns, pd.MultiIndex):
                spy_close = spy_raw["Close"].iloc[:, 0]
            else:
                spy_close = spy_raw["Close"]
            spy_close.index = pd.to_datetime(spy_close.index).date
            spy_close = spy_close[spy_close.index >= sim_days[0]]
            if not spy_close.empty:
                spy_series = (spy_close / spy_close.iloc[0]) * starting_capital
    except Exception:
        pass

    stats = compute_stats(equity_series, trades_df, spy_series, starting_capital, risk_free_rate)
    params = dict(
        starting_capital=starting_capital, base_pct=base_pct,
        max_holding_days=max_holding_days, stop_loss_pct=stop_loss_pct,
        slippage_pct=slippage_pct, risk_free_rate=risk_free_rate,
    )

    return BacktestResult(
        trades_df=trades_df,
        equity_series=equity_series,
        spy_series=spy_series,
        stats=stats,
        params=params,
    )


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def compute_stats(
    equity_series: pd.Series,
    trades_df: pd.DataFrame,
    spy_series: pd.Series,
    starting_capital: float,
    risk_free_rate: float = 0.05,
) -> dict:
    """Compute full performance statistics from equity curve and trade log."""
    if equity_series.empty:
        return {}

    final_value  = float(equity_series.iloc[-1])
    total_ret    = (final_value / starting_capital - 1) * 100
    n_days       = max((equity_series.index[-1] - equity_series.index[0]).days, 1)
    years        = n_days / 365.25
    cagr         = ((final_value / starting_capital) ** (1 / max(years, 1e-6)) - 1) * 100

    daily_rets   = equity_series.pct_change().dropna()
    rf_daily     = (1 + risk_free_rate) ** (1 / 252) - 1
    excess       = daily_rets - rf_daily
    vol          = float(excess.std())

    sharpe   = float(excess.mean() / vol * np.sqrt(252)) if vol > 0 else 0.0
    down     = excess[excess < 0]
    down_std = float(down.std())
    sortino  = float(excess.mean() / down_std * np.sqrt(252)) if len(down) > 1 and down_std > 0 else 0.0

    roll_max = equity_series.cummax()
    dd       = (equity_series - roll_max) / roll_max
    max_dd   = float(dd.min() * 100)
    calmar   = cagr / abs(max_dd) if max_dd != 0 else 0.0

    # Trade-level stats
    if not trades_df.empty:
        n_trades    = len(trades_df)
        wins        = trades_df[trades_df["return_pct"] > 0]
        losses      = trades_df[trades_df["return_pct"] < 0]
        win_rate    = len(wins) / n_trades * 100
        avg_ret     = float(trades_df["return_pct"].mean())
        avg_hold    = float(trades_df["holding_days"].mean())
        gross_wins  = float(wins["return_pct"].sum()) if not wins.empty else 0.0
        gross_loss  = float(abs(losses["return_pct"].sum())) if not losses.empty else 0.0
        pf          = gross_wins / gross_loss if gross_loss > 0 else 999.0
        best_idx    = trades_df["return_pct"].idxmax()
        worst_idx   = trades_df["return_pct"].idxmin()
        best_trade  = {k: v for k, v in trades_df.loc[best_idx].items()
                       if k in ("ticker", "insider_name", "return_pct", "holding_days", "exit_reason")}
        worst_trade = {k: v for k, v in trades_df.loc[worst_idx].items()
                       if k in ("ticker", "insider_name", "return_pct", "holding_days", "exit_reason")}
    else:
        n_trades = win_rate = avg_ret = avg_hold = 0
        pf = 0.0
        best_trade = worst_trade = {}

    # Alpha / Beta vs SPY
    alpha = beta = spy_total = spy_cagr_val = excess_ret = None
    if not spy_series.empty and len(equity_series) > 10:
        combined = pd.DataFrame({
            "port": equity_series, "spy": spy_series
        }).dropna()
        if len(combined) > 10:
            pr = combined["port"].pct_change().dropna()
            sr = combined["spy"].pct_change().dropna()
            al = pd.concat([pr, sr], axis=1).dropna()
            al.columns = ["p", "s"]
            if len(al) > 10 and al["s"].std() > 0:
                beta  = float(al["p"].cov(al["s"]) / al["s"].var())
                alpha = float((al["p"].mean() - beta * al["s"].mean()) * 252 * 100)

        spy_final  = float(spy_series.iloc[-1])
        spy_total  = (spy_final / starting_capital - 1) * 100
        spy_yrs    = max((spy_series.index[-1] - spy_series.index[0]).days / 365.25, 1e-6)
        spy_cagr_val = ((spy_final / starting_capital) ** (1 / spy_yrs) - 1) * 100
        excess_ret = total_ret - spy_total

    return {
        "total_return_pct":    round(total_ret, 2),
        "cagr_pct":            round(cagr, 2),
        "sharpe":              round(sharpe, 3),
        "sortino":             round(sortino, 3),
        "max_drawdown_pct":    round(max_dd, 2),
        "calmar":              round(calmar, 3),
        "win_rate_pct":        round(win_rate, 1),
        "profit_factor":       round(min(pf, 999.0), 2),
        "avg_trade_return_pct": round(avg_ret, 2),
        "avg_holding_days":    round(avg_hold, 1),
        "total_trades":        n_trades,
        "alpha_pct":           round(alpha, 2) if alpha is not None else None,
        "beta":                round(beta, 3) if beta is not None else None,
        "spy_total_return_pct": round(spy_total, 2) if spy_total is not None else None,
        "spy_cagr_pct":        round(spy_cagr_val, 2) if spy_cagr_val is not None else None,
        "excess_return_pct":   round(excess_ret, 2) if excess_ret is not None else None,
        "best_trade":          best_trade,
        "worst_trade":         worst_trade,
        "final_value":         round(final_value, 2),
        "starting_capital":    starting_capital,
        "open_positions":      len([t for t in trades_df["exit_reason"] if t == "OPEN"]) if not trades_df.empty else 0,
    }

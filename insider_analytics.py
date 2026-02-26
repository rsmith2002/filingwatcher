"""
insider_analytics.py

Computes per-insider analytics from section16_filings.csv produced by
sp500_insider_transactions.py.

For each (ticker, insider_name) we calculate:
  • Stock price change since their first transaction in the dataset
  • Weighted average cost basis on voluntary open-market purchases (Code=P)
  • Unrealized P&L on those purchases vs. today's price
  • Total sale proceeds from open-market sales (Code=S)
  • Current reported position value (last known shares_remaining × current price)

Requires: pip install yfinance matplotlib

Usage:
    python insider_analytics.py
    python insider_analytics.py --csv section16_filings.csv
    python insider_analytics.py --no-figures        # skip chart generation
    python insider_analytics.py --show              # pop up interactive windows

Figures produced (saved to ./figures/):
    fig1_price_and_entries.png  — stock price line + insider buy/sell markers
    fig2_unrealized_pnl.png     — unrealized P&L % on open-market purchases
    fig3_position_values.png    — top positions by current market value
"""

import argparse
import math
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CSV = "section16_filings.csv"
OUTPUT_CSV  = "insider_analytics.csv"

# Transaction codes we care about for open-market analysis
CODE_PURCHASE = "P"   # Open-market purchase (voluntary, own money)
CODE_SALE     = "S"   # Open-market sale
CODE_AWARD    = "A"   # Award / grant (cost basis ≈ $0)
CODE_TAX      = "F"   # Shares withheld for tax (automatic disposal)
CODE_EXERCISE = {"M", "X"}  # Option exercise


# ===========================================================================
# Price utilities
# ===========================================================================

def fetch_price_history(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download adjusted close prices for all tickers at once.

    Returns a DataFrame indexed by date with tickers as columns.
    Handles both single and multi-ticker downloads.
    """
    try:
        import yfinance as yf
    except ImportError:
        raise SystemExit(
            "yfinance is required.  Run:  pip install yfinance"
        )

    # Extend end by a day so yfinance includes the end_date itself
    end_extended = str((pd.Timestamp(end_date) + pd.Timedelta(days=1)).date())

    ticker_str = " ".join(tickers)
    print(f"  Downloading price history for: {ticker_str}")
    raw = yf.download(
        ticker_str,
        start=start_date,
        end=end_extended,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )

    if raw.empty:
        return pd.DataFrame()

    # Normalise to a simple DataFrame: index=date, columns=ticker
    if isinstance(raw.columns, pd.MultiIndex):
        # Multi-ticker: raw.columns = (ticker, metric)  or  (metric, ticker)
        # yfinance ≥ 0.2.x with group_by='ticker' → (ticker, metric)
        try:
            closes = raw.xs("Close", axis=1, level=1)
        except KeyError:
            closes = raw["Close"]
    else:
        # Single ticker — columns are metric names ('Close', 'Open', …)
        closes = raw[["Close"]].rename(columns={"Close": tickers[0]})

    closes.index = pd.to_datetime(closes.index).normalize()
    return closes.sort_index()


def price_on_or_after(closes: pd.DataFrame, ticker: str, target_date) -> float | None:
    """Return closing price on target_date or the next available trading day."""
    if ticker not in closes.columns:
        return None
    col = closes[ticker].dropna()
    if col.empty:
        return None
    ts = pd.Timestamp(target_date)
    future = col.index[col.index >= ts]
    if len(future) == 0:
        return None
    return float(col.loc[future[0]])


def latest_price(closes: pd.DataFrame, ticker: str) -> float | None:
    """Return the most recent available closing price for a ticker."""
    if ticker not in closes.columns:
        return None
    col = closes[ticker].dropna()
    if col.empty:
        return None
    return float(col.iloc[-1])


# ===========================================================================
# Per-insider analytics
# ===========================================================================

def _safe_sum(series: pd.Series) -> float | None:
    """Sum, returning None if the series is entirely NaN."""
    s = series.dropna()
    return float(s.sum()) if not s.empty else None


def _wacb(shares: pd.Series, prices: pd.Series) -> float | None:
    """Weighted average cost basis from parallel shares / price arrays."""
    mask = shares.notna() & prices.notna()
    s = shares[mask]
    p = prices[mask]
    if s.empty or s.sum() == 0:
        return None
    return float((s * p).sum() / s.sum())


def compute_insider_analytics(df: pd.DataFrame, closes: pd.DataFrame) -> pd.DataFrame:
    """
    Group section16 rows by (ticker, insider_name) and compute analytics.

    Returns a DataFrame with one row per (ticker, insider_name).
    """
    # Only common-stock rows; derivative rows (options/RSUs) complicate cost basis
    nd = df[df["is_derivative"] == False].copy()

    # Ensure date columns are proper date types
    nd["transaction_date"] = pd.to_datetime(nd["transaction_date"], errors="coerce").dt.date
    nd["filing_date"]      = pd.to_datetime(nd["filing_date"],      errors="coerce").dt.date

    records = []

    for (ticker, insider), grp in nd.groupby(["ticker", "insider_name"], dropna=False):
        if pd.isna(insider) or insider is None:
            continue

        grp = grp.sort_values("transaction_date")

        # ── Identity ──────────────────────────────────────────────────────
        latest_row    = grp.sort_values("filing_date").iloc[-1]
        officer_title = latest_row.get("officer_title")
        is_director   = bool(latest_row.get("is_director"))
        is_officer    = bool(latest_row.get("is_officer"))
        company_name  = latest_row.get("company_name")

        # ── Date anchors ──────────────────────────────────────────────────
        dated_rows   = grp[grp["transaction_date"].notna()]
        first_txn_dt = dated_rows["transaction_date"].min() if not dated_rows.empty else None
        last_fling   = grp["filing_date"].max()

        # ── Prices ────────────────────────────────────────────────────────
        cur_price   = latest_price(closes, ticker)
        entry_price = price_on_or_after(closes, ticker, first_txn_dt) if first_txn_dt else None

        if entry_price and cur_price:
            stock_pct_since_entry = (cur_price - entry_price) / entry_price * 100
        else:
            stock_pct_since_entry = None

        # ── Current position (last reported shares_remaining) ─────────────
        position_row = grp[grp["shares_remaining"].notna()].sort_values("transaction_date")
        last_shares  = float(position_row["shares_remaining"].iloc[-1]) if not position_row.empty else None
        pos_value    = (last_shares * cur_price) if (last_shares is not None and cur_price) else None

        # ── Open-market purchases (Code=P) ────────────────────────────────
        buys        = grp[grp["transaction_code"] == CODE_PURCHASE]
        n_buys      = len(buys)
        buy_shares  = _safe_sum(buys["shares"])
        buy_cost    = _safe_sum(buys["value"])       # sum(shares*price) stored in 'value'
        buy_wacb    = _wacb(buys["shares"], buys["price"])

        if buy_wacb and cur_price and buy_shares:
            buy_unrlzd_pct = (cur_price - buy_wacb) / buy_wacb * 100
            buy_unrlzd_usd = (cur_price - buy_wacb) * buy_shares
        else:
            buy_unrlzd_pct = None
            buy_unrlzd_usd = None

        # ── Open-market sales (Code=S) ─────────────────────────────────────
        sells       = grp[grp["transaction_code"] == CODE_SALE]
        n_sells     = len(sells)
        sell_shares = _safe_sum(sells["shares"])
        sell_proceeds = _safe_sum(sells["value"])

        # Avg sale price
        sell_wacb   = _wacb(sells["shares"], sells["price"])

        # If they also bought on the open market, compare avg sell to avg buy
        if sell_wacb and buy_wacb:
            realized_pnl_per_share = sell_wacb - buy_wacb
            realized_pct           = realized_pnl_per_share / buy_wacb * 100
        else:
            realized_pnl_per_share = None
            realized_pct           = None

        # ── Awards (Code=A) ───────────────────────────────────────────────
        awards        = grp[grp["transaction_code"] == CODE_AWARD]
        award_shares  = _safe_sum(awards["shares"])
        award_cur_val = (award_shares * cur_price) if (award_shares and cur_price) else None

        # ── Tax withholding (Code=F) ──────────────────────────────────────
        tax_sells     = grp[grp["transaction_code"] == CODE_TAX]
        tax_shares    = _safe_sum(tax_sells["shares"])

        # ── Net share activity (acquired - disposed, open market only) ────
        net_open_mkt_shares = (
            (buy_shares or 0) - (sell_shares or 0)
        )

        # ── 10b5-1 plan usage ─────────────────────────────────────────────
        plan_trades = grp[grp["is_10b5_1_plan"] == True]
        pct_plan    = len(plan_trades) / len(grp) * 100 if len(grp) else 0

        records.append({
            # Identity
            "ticker":               ticker,
            "company_name":         company_name,
            "insider_name":         insider,
            "officer_title":        officer_title,
            "is_director":          is_director,
            "is_officer":           is_officer,

            # Timeline
            "first_txn_date":       first_txn_dt,
            "last_filing_date":     last_fling,
            "data_days":            (
                (last_fling - first_txn_dt).days
                if (first_txn_dt and last_fling) else None
            ),

            # Stock performance since first transaction in dataset
            "entry_price":              round(entry_price, 2) if entry_price else None,
            "current_price":            round(cur_price,   2) if cur_price   else None,
            "stock_pct_since_entry":    round(stock_pct_since_entry, 2) if stock_pct_since_entry is not None else None,

            # Current position
            "last_reported_shares":     last_shares,
            "current_position_value":   round(pos_value, 0) if pos_value else None,

            # Open-market purchases
            "n_open_mkt_buys":          n_buys,
            "open_mkt_shares_bought":   round(buy_shares,  0) if buy_shares  else None,
            "open_mkt_total_cost":      round(buy_cost,    0) if buy_cost    else None,
            "open_mkt_wacb":            round(buy_wacb,    4) if buy_wacb    else None,
            "open_mkt_unrealized_pct":  round(buy_unrlzd_pct, 2) if buy_unrlzd_pct is not None else None,
            "open_mkt_unrealized_usd":  round(buy_unrlzd_usd, 0) if buy_unrlzd_usd is not None else None,

            # Open-market sales
            "n_open_mkt_sells":         n_sells,
            "open_mkt_shares_sold":     round(sell_shares, 0)    if sell_shares    else None,
            "open_mkt_total_proceeds":  round(sell_proceeds, 0)  if sell_proceeds  else None,
            "open_mkt_avg_sell_price":  round(sell_wacb, 4)      if sell_wacb      else None,
            "realized_pnl_per_share":   round(realized_pnl_per_share, 4) if realized_pnl_per_share is not None else None,
            "realized_pct":             round(realized_pct, 2)   if realized_pct is not None else None,

            # Awards
            "shares_awarded":           round(award_shares, 0)   if award_shares   else None,
            "award_current_value":      round(award_cur_val, 0)  if award_cur_val  else None,

            # Tax / automatic
            "shares_tax_withheld":      round(tax_shares, 0)     if tax_shares     else None,

            # Net conviction signal
            "net_open_mkt_shares":      round(net_open_mkt_shares, 0),
            "pct_trades_on_10b5_plan":  round(pct_plan, 1),
        })

    return pd.DataFrame(records)


# ===========================================================================
# Summary printers
# ===========================================================================

def _fmt_usd(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "N/A"
    if abs(v) >= 1e9:
        return f"${v/1e9:+.2f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:+.2f}M"
    if abs(v) >= 1e3:
        return f"${v/1e3:+.1f}K"
    return f"${v:+.2f}"


def _fmt_pct(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "N/A"
    return f"{v:+.1f}%"


def _fmt_shares(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "N/A"
    if abs(v) >= 1e6:
        return f"{v/1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:,.0f}"


def print_analytics_summary(ana: pd.DataFrame) -> None:
    print("\n" + "=" * 75)
    print("INSIDER ANALYTICS SUMMARY")
    print("=" * 75)
    print(f"Insiders analysed  : {len(ana):,}")
    print(f"Tickers            : {', '.join(sorted(ana['ticker'].unique()))}")
    print()

    # ── Stock performance since entry ─────────────────────────────────────
    print("─" * 75)
    print("STOCK PERFORMANCE SINCE EACH INSIDER'S FIRST TRANSACTION IN DATASET")
    print("─" * 75)
    perf = (
        ana[ana["stock_pct_since_entry"].notna()]
           .sort_values("stock_pct_since_entry", ascending=False)
    )
    hdr = f"  {'Insider':<35} {'Ticker':<7} {'Entry Date':<12} {'Entry $':>8} {'Now $':>8} {'Chg':>8}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for _, row in perf.iterrows():
        print(
            f"  {str(row.insider_name):<35} {row.ticker:<7} "
            f"{str(row.first_txn_date):<12} "
            f"{row.entry_price:>8.2f} "
            f"{row.current_price:>8.2f} "
            f"{_fmt_pct(row.stock_pct_since_entry):>8}"
        )

    # ── Open-market buyers ────────────────────────────────────────────────
    buyers = ana[ana["n_open_mkt_buys"] > 0].sort_values(
        "open_mkt_unrealized_usd", ascending=False
    )
    if not buyers.empty:
        print()
        print("─" * 75)
        print("OPEN-MARKET BUYERS  (Code=P, voluntary purchases with own money)")
        print("─" * 75)
        hdr2 = (
            f"  {'Insider':<35} {'Ticker':<7} {'Shares Bought':>14} "
            f"{'Avg Cost':>9} {'Now':>8} {'Unrlzd %':>9} {'Unrlzd $':>12}"
        )
        print(hdr2)
        print("  " + "-" * (len(hdr2) - 2))
        for _, row in buyers.iterrows():
            print(
                f"  {str(row.insider_name):<35} {row.ticker:<7} "
                f"{_fmt_shares(row.open_mkt_shares_bought):>14} "
                f"{row.open_mkt_wacb:>9.2f} "
                f"{row.current_price:>8.2f} "
                f"{_fmt_pct(row.open_mkt_unrealized_pct):>9} "
                f"{_fmt_usd(row.open_mkt_unrealized_usd):>12}"
            )

    # ── Open-market sellers ───────────────────────────────────────────────
    sellers = ana[ana["n_open_mkt_sells"] > 0].sort_values(
        "open_mkt_total_proceeds", ascending=False
    )
    if not sellers.empty:
        print()
        print("─" * 75)
        print("OPEN-MARKET SELLERS  (Code=S, voluntary sales)")
        print("─" * 75)
        hdr3 = (
            f"  {'Insider':<35} {'Ticker':<7} {'Shares Sold':>12} "
            f"{'Avg Price':>10} {'Proceeds':>14} {'vs Buy WACB':>12}"
        )
        print(hdr3)
        print("  " + "-" * (len(hdr3) - 2))
        for _, row in sellers.iterrows():
            vs_buy = _fmt_pct(row.realized_pct) if row.realized_pct is not None else "N/A (no P)"
            print(
                f"  {str(row.insider_name):<35} {row.ticker:<7} "
                f"{_fmt_shares(row.open_mkt_shares_sold):>12} "
                f"  ${row.open_mkt_avg_sell_price:>8.2f} "
                f"{_fmt_usd(row.open_mkt_total_proceeds):>14} "
                f"{vs_buy:>12}"
            )

    # ── Current positions ─────────────────────────────────────────────────
    positions = ana[ana["current_position_value"].notna()].sort_values(
        "current_position_value", ascending=False
    )
    if not positions.empty:
        print()
        print("─" * 75)
        print("LARGEST REPORTED POSITIONS  (shares_remaining × current price)")
        print("─" * 75)
        hdr4 = (
            f"  {'Insider':<35} {'Ticker':<7} {'Title':<25} "
            f"{'Shares':>12} {'Position Value':>16}"
        )
        print(hdr4)
        print("  " + "-" * (len(hdr4) - 2))
        for _, row in positions.head(20).iterrows():
            title = str(row.officer_title or "")[:24]
            print(
                f"  {str(row.insider_name):<35} {row.ticker:<7} {title:<25} "
                f"{_fmt_shares(row.last_reported_shares):>12} "
                f"{_fmt_usd(row.current_position_value):>16}"
            )

    # ── Net conviction ────────────────────────────────────────────────────
    print()
    print("─" * 75)
    print("NET OPEN-MARKET CONVICTION  (P buys - S sells, shares)")
    print("─" * 75)
    active = ana[(ana["n_open_mkt_buys"] > 0) | (ana["n_open_mkt_sells"] > 0)].copy()
    active = active.sort_values("net_open_mkt_shares", ascending=False)
    hdr5 = (
        f"  {'Insider':<35} {'Ticker':<7} {'Buys':>8} {'Sells':>8} {'Net Shares':>12}"
    )
    print(hdr5)
    print("  " + "-" * (len(hdr5) - 2))
    for _, row in active.iterrows():
        bought = _fmt_shares(row.open_mkt_shares_bought or 0)
        sold   = _fmt_shares(row.open_mkt_shares_sold or 0)
        net    = _fmt_shares(row.net_open_mkt_shares)
        sign   = "▲ BUY" if row.net_open_mkt_shares > 0 else ("▼ SELL" if row.net_open_mkt_shares < 0 else "")
        print(
            f"  {str(row.insider_name):<35} {row.ticker:<7} "
            f"{bought:>8} {sold:>8} {net:>12}  {sign}"
        )

    print()
    print("─" * 75)
    print("10b5-1 PLAN USAGE  (pre-planned trades — less informative as signal)")
    print("─" * 75)
    plan_users = ana[ana["pct_trades_on_10b5_plan"] > 0].sort_values(
        "pct_trades_on_10b5_plan", ascending=False
    )
    if plan_users.empty:
        print("  No 10b5-1 plan trades detected.")
    else:
        for _, row in plan_users.iterrows():
            print(
                f"  {str(row.insider_name):<35} {row.ticker:<7} "
                f"{row.pct_trades_on_10b5_plan:.0f}% of trades on 10b5-1 plan"
            )

    print("=" * 75)


# ===========================================================================
# Figures
# ===========================================================================

def _fig_price_with_transactions(df: pd.DataFrame, closes: pd.DataFrame):
    """
    Figure 1: One subplot per ticker showing the stock price line with insider
    buy (▲) and sell (▼) markers positioned at the actual transaction price.
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.lines import Line2D

    # Only open-market, non-derivative, dated, priced rows
    mkt = df[
        df["transaction_code"].isin(["P", "S"])
        & df["transaction_date"].notna()
        & df["price"].notna()
        & (df["is_derivative"] == False)
    ].copy()
    mkt["transaction_date"] = pd.to_datetime(mkt["transaction_date"])

    tickers = sorted(df["ticker"].dropna().unique())
    ncols = min(3, len(tickers))
    nrows = math.ceil(len(tickers) / ncols)

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(8 * ncols, 5 * nrows),
        squeeze=False,
    )
    fig.suptitle(
        "Stock Price with Insider Entry Points\n"
        "▲ Open-market Buy (Code=P)   ▼ Open-market Sell (Code=S)",
        fontsize=13, fontweight="bold", y=1.01,
    )

    # Assign a stable colour to each insider across all subplots
    all_insiders = mkt["insider_name"].dropna().unique()
    palette = (
        list(plt.cm.tab20.colors)
        + list(plt.cm.tab20b.colors)
        + list(plt.cm.tab20c.colors)
    )
    insider_color = {name: palette[i % len(palette)] for i, name in enumerate(all_insiders)}

    flat_axes = [ax for row in axes for ax in row]

    for ax, ticker in zip(flat_axes, tickers):
        # ── Price line ───────────────────────────────────────────────────
        if ticker in closes.columns:
            price_series = closes[ticker].dropna()
            ax.plot(
                price_series.index, price_series.values,
                color="#4C72B0", linewidth=1.6, alpha=0.85, zorder=1,
            )
            cur = float(price_series.iloc[-1])
            ax.axhline(cur, color="#888", linestyle="--", linewidth=0.8, alpha=0.6)
            ax.annotate(
                f"${cur:.2f}",
                xy=(price_series.index[-1], cur),
                xytext=(-6, 5), textcoords="offset points",
                fontsize=7, ha="right", color="#555",
            )

        # ── Transaction markers ──────────────────────────────────────────
        ticker_txns = mkt[mkt["ticker"] == ticker]
        plotted = set()

        for _, row in ticker_txns.iterrows():
            insider = row["insider_name"]
            if pd.isna(insider):
                continue
            color  = insider_color.get(insider, "#999")
            dt     = row["transaction_date"]
            price  = row["price"]
            is_buy = row["transaction_code"] == "P"

            ax.scatter(
                dt, price,
                marker="^" if is_buy else "v",
                color=color,
                s=90, zorder=5,
                edgecolors="#1a7a1a" if is_buy else "#aa1a1a",
                linewidths=0.8,
            )
            plotted.add(insider)

        # ── Per-subplot legend ───────────────────────────────────────────
        legend_handles = [
            Line2D([0], [0], marker="^", color="w", markerfacecolor="#aaa",
                   markersize=7, markeredgecolor="#1a7a1a", label="Buy (P)"),
            Line2D([0], [0], marker="v", color="w", markerfacecolor="#aaa",
                   markersize=7, markeredgecolor="#aa1a1a", label="Sell (S)"),
        ]
        for ins in sorted(plotted):
            legend_handles.append(
                Line2D([0], [0], marker="o", color="w",
                       markerfacecolor=insider_color[ins],
                       markersize=7, label=ins[:28])
            )

        ax.set_title(ticker, fontweight="bold", fontsize=12, pad=6)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=35, ha="right", fontsize=7)
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x:,.0f}")
        )
        ax.tick_params(axis="y", labelsize=7)
        ax.grid(True, alpha=0.25, linestyle=":")
        if plotted:
            ax.legend(
                handles=legend_handles,
                fontsize=6, loc="upper left",
                framealpha=0.8, ncol=1,
            )

    # Hide surplus subplots
    for ax in flat_axes[len(tickers):]:
        ax.set_visible(False)

    fig.tight_layout()
    return fig


def _fig_unrealized_pnl(ana: pd.DataFrame):
    """
    Figure 2 (adaptive):

    If insiders made open-market purchases (Code=P):
        Horizontal bar chart of unrealized P&L % on those purchases.
        Green = in profit, red = underwater.
        Annotated with WACB → current price and dollar gain/loss.

    Fallback — if nobody bought on the open market (common for large-cap tech
    where execs only receive RSU awards then sell):
        "Sale timing" chart: avg sale price vs today's price per insider.
        Negative % = they sold before a DROP  → smart timing (green).
        Positive %  = they sold before a RISE → left gains on the table (red).
    """
    import matplotlib.pyplot as plt

    # ── Primary view: open-market purchases ─────────────────────────────
    buyers = ana[
        ana["open_mkt_wacb"].notna() & ana["open_mkt_unrealized_pct"].notna()
    ].copy().sort_values("open_mkt_unrealized_pct", ascending=True)

    if not buyers.empty:
        labels = buyers.apply(
            lambda r: f"{str(r.insider_name)[:28]}  ({r.ticker})", axis=1
        ).tolist()
        pcts  = buyers["open_mkt_unrealized_pct"].tolist()
        usds  = buyers["open_mkt_unrealized_usd"].tolist()
        wacbs = buyers["open_mkt_wacb"].tolist()
        curs  = buyers["current_price"].tolist()

        colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in pcts]
        fig_h  = max(4, len(buyers) * 0.55)
        fig, ax = plt.subplots(figsize=(11, fig_h))
        bars = ax.barh(labels, pcts, color=colors, edgecolor="white", height=0.65)
        ax.axvline(0, color="#333", linewidth=1.1)

        for bar, pct, usd, wacb, cur in zip(bars, pcts, usds, wacbs, curs):
            if usd is not None and not (isinstance(usd, float) and math.isnan(usd)):
                note = f"  WACB ${wacb:.2f} → ${cur:.2f}  ({_fmt_usd(usd)})"
                x    = bar.get_width()
                ha   = "left" if pct >= 0 else "right"
                ax.text(x + (0.4 if pct >= 0 else -0.4),
                        bar.get_y() + bar.get_height() / 2,
                        note, va="center", ha=ha, fontsize=7, color="#333")

        xmax = max(abs(v) for v in pcts) * 1.6
        ax.set_xlim(-xmax, xmax)
        ax.set_xlabel("Unrealized Gain / Loss on Open-Market Purchases (%)", fontsize=10)
        ax.set_title(
            "Unrealized P&L — Open-Market Purchases (Code=P)\n"
            "Cost basis = weighted avg price paid; gain/loss vs today's price",
            fontsize=12, fontweight="bold",
        )
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(True, axis="x", alpha=0.25, linestyle=":")
        fig.tight_layout()
        return fig

    # ── Fallback view: sale timing ────────────────────────────────────────
    # No open-market purchases exist; show how well insiders timed their sales.
    # pct = (current_price - avg_sell_price) / avg_sell_price
    #   negative → stock fell after they sold  → smart / good timing  (green)
    #   positive → stock rose after they sold  → they left gains      (orange/red)
    sellers = ana[
        ana["open_mkt_avg_sell_price"].notna() & ana["current_price"].notna()
    ].copy()

    if sellers.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5,
                "No open-market purchase or sale data found.\n"
                "Check that section16_filings.csv contains Code=P or Code=S rows.",
                ha="center", va="center", fontsize=10, color="#555")
        ax.axis("off")
        return fig

    sellers["timing_pct"] = (
        (sellers["current_price"] - sellers["open_mkt_avg_sell_price"])
        / sellers["open_mkt_avg_sell_price"] * 100
    )
    sellers = sellers.sort_values("timing_pct", ascending=True)

    labels  = sellers.apply(
        lambda r: f"{str(r.insider_name)[:28]}  ({r.ticker})", axis=1
    ).tolist()
    pcts    = sellers["timing_pct"].tolist()
    avgs    = sellers["open_mkt_avg_sell_price"].tolist()
    curs    = sellers["current_price"].tolist()
    procs   = sellers["open_mkt_total_proceeds"].tolist()

    # Negative = good timing (green), positive = left gains on table (orange-red)
    colors = ["#27ae60" if v <= 0 else "#e67e22" if v <= 20 else "#e74c3c"
              for v in pcts]

    fig_h = max(4, len(sellers) * 0.55)
    fig, ax = plt.subplots(figsize=(12, fig_h))
    bars = ax.barh(labels, pcts, color=colors, edgecolor="white", height=0.65)
    ax.axvline(0, color="#333", linewidth=1.1)

    for bar, pct, avg, cur, proc in zip(bars, pcts, avgs, curs, procs):
        proc_str = _fmt_usd(proc) if proc else "?"
        note = f"  Sold avg ${avg:.2f} → now ${cur:.2f}  (proceeds {proc_str})"
        x  = bar.get_width()
        ha = "left" if pct >= 0 else "right"
        ax.text(x + (0.3 if pct >= 0 else -0.3),
                bar.get_y() + bar.get_height() / 2,
                note, va="center", ha=ha, fontsize=7, color="#333")

    xmax = max(abs(v) for v in pcts) * 1.7 if pcts else 10
    ax.set_xlim(-xmax, xmax)
    ax.set_xlabel(
        "Stock move since sale  (negative = stock fell → good timing, "
        "positive = stock rose → left gains on table)  %",
        fontsize=9,
    )
    ax.set_title(
        "Insider Sale Timing vs. Today's Price  (Code=S)\n"
        "No open-market purchases found — these insiders sell RSU awards, not buy on market",
        fontsize=12, fontweight="bold",
    )
    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, axis="x", alpha=0.25, linestyle=":")

    # Colour legend
    from matplotlib.patches import Patch
    ax.legend(handles=[
        Patch(facecolor="#27ae60", label="Stock fell after sale (timed well)"),
        Patch(facecolor="#e67e22", label="Stock rose ≤20% after sale"),
        Patch(facecolor="#e74c3c", label="Stock rose >20% after sale (left gains)"),
    ], fontsize=8, loc="lower right")

    fig.tight_layout()
    return fig


def _fig_position_values(ana: pd.DataFrame):
    """
    Figure 3: Horizontal bar chart — top reported insider positions by current
    market value, coloured by ticker.
    """
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    pos = (
        ana[ana["current_position_value"].notna()]
        .sort_values("current_position_value", ascending=True)
        .tail(25)
    )
    if pos.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No position data", ha="center", va="center")
        return fig

    labels = pos.apply(
        lambda r: f"{str(r.insider_name)[:28]}  ({r.ticker})", axis=1
    ).tolist()
    values_m = (pos["current_position_value"] / 1e6).tolist()  # in $M

    # Colour by ticker
    tickers_order = pos["ticker"].unique()
    palette = list(plt.cm.Set2.colors) + list(plt.cm.Set3.colors)
    ticker_color = {t: palette[i % len(palette)] for i, t in enumerate(tickers_order)}
    colors = [ticker_color[t] for t in pos["ticker"]]

    fig_h = max(4, len(pos) * 0.48)
    fig, ax = plt.subplots(figsize=(11, fig_h))

    bars = ax.barh(labels, values_m, color=colors, edgecolor="white", height=0.7)

    for bar, val, shares, title in zip(
        bars, values_m,
        pos["last_reported_shares"].tolist(),
        pos["officer_title"].tolist(),
    ):
        shares_str = _fmt_shares(shares) if shares else "?"
        title_str  = str(title or "")[:22]
        ax.text(
            bar.get_width() + max(values_m) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"${val:.1f}M  ({shares_str} sh)  {title_str}",
            va="center", ha="left", fontsize=7, color="#333",
        )

    ax.set_xlim(0, max(values_m) * 1.55)
    ax.set_xlabel("Current Position Value (USD millions)", fontsize=10)
    ax.set_title(
        "Top Insider Positions by Current Market Value\n"
        "Last reported shares × current price",
        fontsize=12, fontweight="bold",
    )
    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, axis="x", alpha=0.25, linestyle=":")

    legend_handles = [
        Patch(facecolor=ticker_color[t], label=t) for t in tickers_order
    ]
    ax.legend(handles=legend_handles, fontsize=8, loc="lower right")
    fig.tight_layout()
    return fig


def generate_figures(
    df: pd.DataFrame,
    ana: pd.DataFrame,
    closes: pd.DataFrame,
    output_dir: Path,
    show: bool = False,
) -> list[Path]:
    """
    Generate and save all figures.  Returns paths of saved files.

    Args:
        df          — raw section16 rows
        ana         — per-insider analytics DataFrame
        closes      — price history (date index, ticker columns)
        output_dir  — directory to write PNG files into
        show        — if True, call plt.show() (blocks until window closed)
    """
    try:
        import matplotlib
        if not show:
            matplotlib.use("Agg")   # non-interactive backend for file-only output
        import matplotlib.pyplot as plt
    except ImportError:
        raise SystemExit("matplotlib is required.  Run:  pip install matplotlib")

    output_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    print("\nGenerating figures …")

    # ── Figure 1 ─────────────────────────────────────────────────────────
    print("  fig1: stock price + insider entries")
    fig1 = _fig_price_with_transactions(df, closes)
    p1 = output_dir / "fig1_price_and_entries.png"
    fig1.savefig(p1, dpi=150, bbox_inches="tight")
    saved.append(p1)
    plt.close(fig1)

    # ── Figure 2 ─────────────────────────────────────────────────────────
    print("  fig2: unrealized P&L on open-market purchases")
    fig2 = _fig_unrealized_pnl(ana)
    p2 = output_dir / "fig2_unrealized_pnl.png"
    fig2.savefig(p2, dpi=150, bbox_inches="tight")
    saved.append(p2)
    plt.close(fig2)

    # ── Figure 3 ─────────────────────────────────────────────────────────
    print("  fig3: top positions by current value")
    fig3 = _fig_position_values(ana)
    p3 = output_dir / "fig3_position_values.png"
    fig3.savefig(p3, dpi=150, bbox_inches="tight")
    saved.append(p3)
    plt.close(fig3)

    print(f"\nFigures saved → {output_dir.resolve()}/")
    for p in saved:
        print(f"  {p.name}")

    if show:
        plt.show()

    return saved


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Compute insider analytics from section16_filings.csv")
    parser.add_argument(
        "--csv", default=DEFAULT_CSV,
        help=f"Path to section16 CSV file (default: {DEFAULT_CSV})"
    )
    parser.add_argument(
        "--out", default=OUTPUT_CSV,
        help=f"Output analytics CSV (default: {OUTPUT_CSV})"
    )
    parser.add_argument(
        "--figures-dir", default="figures",
        help="Directory for output PNG figures (default: ./figures)"
    )
    parser.add_argument(
        "--no-figures", action="store_true",
        help="Skip figure generation"
    )
    parser.add_argument(
        "--show", action="store_true",
        help="Display figures in interactive windows (blocks until closed)"
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(
            f"ERROR: {csv_path} not found.\n"
            "Run sp500_insider_transactions.py first to generate the data."
        )

    print(f"Loading {csv_path} …")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  {len(df):,} rows, {df['ticker'].nunique()} tickers, "
          f"{df['insider_name'].nunique()} insiders")

    # Date bounds for price download
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
    df["filing_date"]      = pd.to_datetime(df["filing_date"], errors="coerce").dt.date

    dated = df[df["transaction_date"].notna()]
    if dated.empty:
        raise SystemExit("No rows with transaction_date — cannot compute analytics.")

    price_start = str(dated["transaction_date"].min())
    price_end   = str(date.today() + timedelta(days=1))   # ensure today included

    tickers = sorted(df["ticker"].dropna().unique().tolist())

    print(f"\nFetching price history: {price_start} → {date.today()}")
    closes = fetch_price_history(tickers, price_start, price_end)
    print(f"  Got {len(closes)} trading days, {closes.shape[1]} tickers")

    print("\nComputing per-insider analytics …")
    ana = compute_insider_analytics(df, closes)
    print(f"  {len(ana)} insider records computed")

    # Save
    out_path = Path(args.out)
    ana.to_csv(out_path, index=False)
    print(f"\nSaved analytics → {out_path.resolve()}")

    # Print summary tables
    print_analytics_summary(ana)

    # Figures
    if not args.no_figures:
        generate_figures(
            df=df,
            ana=ana,
            closes=closes,
            output_dir=Path(args.figures_dir),
            show=args.show,
        )


if __name__ == "__main__":
    main()

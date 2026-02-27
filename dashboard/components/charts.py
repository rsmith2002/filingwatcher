"""
dashboard/components/charts.py

Plotly figure builders used by the dashboard callbacks.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


THEME = dict(
    bg        = "#080810",
    paper     = "#0d0e1b",
    grid      = "#1c1e30",
    text      = "#c6bead",
    buy_col   = "#17d890",
    sell_col  = "#ff3d5a",
    line_col  = "#5b8fff",
    award_col = "#f0a31a",
)

_MONO = "'JetBrains Mono', ui-monospace, monospace"

_LAYOUT_BASE = dict(
    paper_bgcolor = THEME["paper"],
    plot_bgcolor  = THEME["bg"],
    font          = dict(color=THEME["text"], family=_MONO, size=11),
    margin        = dict(l=60, r=20, t=50, b=50),
    legend        = dict(bgcolor="rgba(0,0,0,0)", bordercolor=THEME["grid"],
                         borderwidth=1, font=dict(size=11)),
    xaxis         = dict(gridcolor=THEME["grid"], showgrid=True, zeroline=False,
                         tickfont=dict(family=_MONO, size=10), linecolor=THEME["grid"]),
    yaxis         = dict(gridcolor=THEME["grid"], showgrid=True, zeroline=False,
                         tickfont=dict(family=_MONO, size=10), linecolor=THEME["grid"]),
)


def price_with_transactions(
    ticker: str,
    prices_df: pd.DataFrame,            # date-indexed, 'close' column
    filings_df: pd.DataFrame,           # filtered section16 rows for this ticker
    selected_insider: str | None = None,
) -> go.Figure:
    """
    Stock price line with buy/sell/award markers for every insider.
    Hover shows: insider name, role, shares, price, transaction type.
    """
    fig = go.Figure()

    # ── Price line ────────────────────────────────────────────────────────
    if not prices_df.empty:
        fig.add_trace(go.Scatter(
            x=prices_df.index,
            y=prices_df["close"],
            mode="lines",
            name=ticker,
            line=dict(color=THEME["line_col"], width=2),
            hovertemplate="%{x|%b %d %Y}<br>$%{y:.2f}<extra></extra>",
        ))

    # ── Transaction markers ───────────────────────────────────────────────
    # Only open-market + priced, non-derivative rows
    # Guard: empty DataFrame (no columns) when no filings exist yet
    if filings_df.empty or "transaction_code" not in filings_df.columns:
        mkt = pd.DataFrame(columns=["transaction_code", "transaction_date",
                                    "is_derivative", "insider_name",
                                    "officer_title", "shares", "price"])
    else:
        mkt = filings_df[
            filings_df["transaction_code"].isin(["P", "S", "A"])
            & filings_df["transaction_date"].notna()
            & (filings_df["is_derivative"] == False)
        ].copy()
    mkt["transaction_date"] = pd.to_datetime(mkt["transaction_date"])

    code_config = {
        "P": dict(symbol="triangle-up",   color=THEME["buy_col"],  size=10, name="Buy (P)"),
        "S": dict(symbol="triangle-down", color=THEME["sell_col"], size=10, name="Sell (S)"),
        "A": dict(symbol="star",          color=THEME["award_col"], size=9,  name="Award (A)"),
    }

    has_selection = bool(selected_insider)

    for code, cfg in code_config.items():
        subset = mkt[mkt["transaction_code"] == code]
        if subset.empty:
            continue
        # Always pin markers to the nearest stock close so they sit on the
        # price line regardless of stock splits (Form 4 prices are unadjusted).
        # The actual transaction price is shown in the hover tooltip.
        y_vals = []
        for _, row in subset.iterrows():
            if not prices_df.empty:
                dt = row["transaction_date"]
                future = prices_df.index[prices_df.index >= dt]
                y_vals.append(float(prices_df.loc[future[0], "close"]) if len(future) else None)
            else:
                y_vals.append(None)

        # Per-marker styling based on selected insider
        is_sel = (subset["insider_name"] == selected_insider) if has_selection else pd.Series([False] * len(subset), index=subset.index)
        marker_sizes    = [cfg["size"] * 1.8 if (has_selection and s) else cfg["size"] for s in is_sel]
        marker_opacities = [1.0 if (not has_selection or s) else 0.15 for s in is_sel]
        marker_lines    = [
            dict(color=THEME["award_col"], width=2) if (has_selection and s) else dict(color="white", width=0.5)
            for s in is_sel
        ]

        hover = (
            subset["insider_name"].fillna("Unknown") + "<br>"
            + subset["officer_title"].fillna("").apply(lambda x: f"{x}<br>" if x else "")
            + subset["shares"].apply(lambda x: f"{x:,.0f} shares" if pd.notna(x) else "")
            + subset["price"].apply(lambda x: f" @ ${x:.2f}" if pd.notna(x) and x > 0 else "")
            + "<br>" + subset["transaction_date"].dt.strftime("%b %d %Y")
        )
        fig.add_trace(go.Scatter(
            x=subset["transaction_date"],
            y=y_vals,
            mode="markers",
            name=cfg["name"],
            marker=dict(
                symbol=cfg["symbol"],
                color=cfg["color"],
                size=marker_sizes,
                opacity=marker_opacities,
                line=marker_lines,
            ),
            text=hover,
            hovertemplate="%{text}<extra></extra>",
        ))

    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=f"<b>{ticker}</b> — Insider Transactions", x=0.01,
                   font=dict(size=16)),
        hovermode="x unified",
        height=420,
    )
    return fig


def unrealized_pnl_bar(analytics_df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar — unrealized P&L % on open-market purchases, or
    sale-timing view if no purchases exist.
    """
    if analytics_df.empty or "open_mkt_wacb" not in analytics_df.columns:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title="No analytics data yet.")
        return fig

    buyers = analytics_df[
        analytics_df["open_mkt_wacb"].notna()
        & analytics_df["open_mkt_unrealized_pct"].notna()
    ].sort_values("open_mkt_unrealized_pct")

    if not buyers.empty:
        labels = (buyers["insider_name"].str[:28] + "  (" + buyers["ticker"] + ")").tolist()
        pcts   = buyers["open_mkt_unrealized_pct"].tolist()
        colors = [THEME["buy_col"] if v >= 0 else THEME["sell_col"] for v in pcts]
        hovers = [
            f"WACB ${row.open_mkt_wacb:.2f} → ${row.current_price:.2f}<br>"
            f"Unrealized: ${row.open_mkt_unrealized_usd:+,.0f}"
            for _, row in buyers.iterrows()
        ]
        title = "<b>Unrealized P&L</b> — Open-Market Purchases (Code=P)"
        x_title = "Unrealized Gain / Loss (%)"
    else:
        # Fallback: sale timing
        sellers = analytics_df[
            analytics_df["open_mkt_avg_sell_price"].notna()
            & analytics_df["current_price"].notna()
        ].copy()
        if sellers.empty:
            fig = go.Figure()
            fig.update_layout(**_LAYOUT_BASE,
                              title="No open-market purchase or sale data available.")
            return fig
        sellers["timing_pct"] = (
            (sellers["current_price"] - sellers["open_mkt_avg_sell_price"])
            / sellers["open_mkt_avg_sell_price"] * 100
        )
        sellers = sellers.sort_values("timing_pct")
        labels = (sellers["insider_name"].str[:28] + "  (" + sellers["ticker"] + ")").tolist()
        pcts   = sellers["timing_pct"].tolist()
        colors = [THEME["buy_col"] if v <= 0 else THEME["sell_col"] for v in pcts]
        hovers = [
            f"Sold avg ${row.open_mkt_avg_sell_price:.2f} → now ${row.current_price:.2f}<br>"
            f"Proceeds: ${row.open_mkt_total_proceeds:+,.0f}"
            if pd.notna(row.open_mkt_total_proceeds) else
            f"Sold avg ${row.open_mkt_avg_sell_price:.2f} → now ${row.current_price:.2f}"
            for _, row in sellers.iterrows()
        ]
        title   = "<b>Sale Timing</b> — Stock move since insider sold (negative = good timing)"
        x_title = "Stock % change since sale"

    fig = go.Figure(go.Bar(
        x=pcts, y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.1f}%" for v in pcts],
        textposition="outside",
        customdata=hovers,
        hovertemplate="%{customdata}<extra></extra>",
    ))
    fig.add_vline(x=0, line_width=1, line_color=THEME["text"])
    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, x=0.01, font=dict(size=14)),
        xaxis_title=x_title,
        height=max(350, len(labels) * 36 + 80),
        bargap=0.3,
    )
    return fig


def position_values_bar(analytics_df: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """Top insider positions by current market value."""
    if analytics_df.empty or "current_position_value" not in analytics_df.columns:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title="No position data available.")
        return fig

    pos = (
        analytics_df[analytics_df["current_position_value"].notna()]
        .sort_values("current_position_value", ascending=True)
        .tail(top_n)
    )
    if pos.empty:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title="No position data available.")
        return fig

    labels   = (pos["insider_name"].str[:28] + "  (" + pos["ticker"] + ")").tolist()
    values_m = (pos["current_position_value"] / 1e6).tolist()
    colors   = [THEME["buy_col"] if t == pos["ticker"].iloc[-1] else "#4c8eff"
                for t in pos["ticker"]]

    ticker_palette = {t: c for t, c in zip(
        pos["ticker"].unique(),
        ["#4c8eff", "#00d4aa", "#ff9f43", "#ff4d6d", "#a29bfe",
         "#fd79a8", "#55efc4", "#ffeaa7", "#74b9ff", "#e17055"]
    )}
    colors = [ticker_palette.get(t, "#4c8eff") for t in pos["ticker"]]

    hovers = [
        f"{row.insider_name}<br>{row.officer_title or ''}<br>"
        f"Shares: {row.last_reported_shares:,.0f}<br>"
        f"Position: ${row.current_position_value / 1e6:.1f}M"
        for _, row in pos.iterrows()
    ]

    fig = go.Figure(go.Bar(
        x=values_m, y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"${v:.1f}M" for v in values_m],
        textposition="outside",
        customdata=hovers,
        hovertemplate="%{customdata}<extra></extra>",
    ))
    fig.update_layout(**_LAYOUT_BASE)
    fig.update_layout(
        title=dict(text="<b>Largest Reported Positions</b> (shares × current price)",
                   x=0.01, font=dict(size=14)),
        xaxis_title="Position Value ($M)",
        height=max(350, len(labels) * 36 + 80),
        bargap=0.3,
        xaxis_range=[0, max(values_m) * 1.3],
    )
    return fig


def return_window_scatter(
    analytics_df: pd.DataFrame,
    window: str = "3m",
    selected_insider: str | None = None,
) -> go.Figure:
    """
    Scatter: x = entry_price, y = return in selected window.
    Bubble size = position value. Color = buy_col / sell_col by sign.
    If selected_insider is set, that bubble is highlighted with an amber ring.
    """
    col = f"pct_{window}"
    if analytics_df.empty or col not in analytics_df.columns:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title=f"No return data for {window} window.")
        return fig

    df = analytics_df[analytics_df[col].notna() & analytics_df["entry_price"].notna()].copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title=f"No return data for {window} window.")
        return fig

    df["bubble_size"] = (
        df["current_position_value"].fillna(1e6).clip(lower=1e5) / 1e6
    ).clip(upper=50).pow(0.5) * 8

    is_selected = (df["insider_name"] == selected_insider) if selected_insider else pd.Series([False] * len(df), index=df.index)
    has_selection = selected_insider and is_selected.any()

    colors  = df[col].apply(lambda v: THEME["buy_col"] if v >= 0 else THEME["sell_col"])
    opacity = df.index.map(lambda i: 1.0 if (not has_selection or is_selected.loc[i]) else 0.2)
    sizes   = df.index.map(lambda i: df.loc[i, "bubble_size"] * (1.8 if (has_selection and is_selected.loc[i]) else 1.0))
    borders = df.index.map(lambda i:
        dict(color=THEME["award_col"], width=2.5) if (has_selection and is_selected.loc[i])
        else dict(color="white", width=0.5)
    )

    hovers = (
        df["insider_name"] + "<br>"
        + df["ticker"] + " | " + df["officer_title"].fillna("") + "<br>"
        + df[col].apply(lambda v: f"Return ({window}): {v:+.1f}%") + "<br>"
        + df["first_txn_date"].astype(str).apply(lambda d: f"Entry: {d}")
    )

    fig = go.Figure(go.Scatter(
        x=df["entry_price"],
        y=df[col],
        mode="markers+text",
        marker=dict(
            color=colors.tolist(),
            size=list(sizes),
            line=[b for b in borders],
            opacity=list(opacity),
        ),
        text=df["ticker"],
        textposition="top center",
        textfont=dict(size=9),
        customdata=hovers,
        hovertemplate="%{customdata}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color=THEME["grid"], line_width=1)
    title_text = (
        f"<b>Entry Price vs {window} Return</b> — {selected_insider}"
        if has_selection else
        f"<b>Entry Price vs {window} Return</b> — bubble size = position value"
    )
    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title_text, x=0.01, font=dict(size=14)),
        xaxis_title="Entry Price ($)",
        yaxis_title=f"Return over {window} (%)",
        height=420,
    )
    return fig


def activity_timeline(filings_df: pd.DataFrame) -> go.Figure:
    """
    Bar chart: daily count of filings, coloured by transaction_code.
    """
    if filings_df.empty:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, title="No filing activity data.")
        return fig

    df = filings_df.copy()
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    df["code_group"] = df["transaction_code"].map({
        "P": "Buy (P)", "S": "Sell (S)", "A": "Award (A)",
        "F": "Tax (F)", "M": "Exercise (M)", "X": "Exercise (X)",
    }).fillna("Other")

    color_map = {
        "Buy (P)":    THEME["buy_col"],
        "Sell (S)":   THEME["sell_col"],
        "Award (A)":  THEME["award_col"],
        "Tax (F)":    "#a29bfe",
        "Exercise (M)": "#74b9ff",
        "Exercise (X)": "#55efc4",
        "Other":      "#636e72",
    }

    fig = go.Figure()
    for group in ["Buy (P)", "Sell (S)", "Award (A)", "Tax (F)",
                  "Exercise (M)", "Exercise (X)", "Other"]:
        sub = df[df["code_group"] == group].groupby("filing_date").size().reset_index(name="count")
        if sub.empty:
            continue
        fig.add_trace(go.Bar(
            x=sub["filing_date"], y=sub["count"],
            name=group,
            marker_color=color_map[group],
            hovertemplate=f"{group}: %{{y}} filings on %{{x|%b %d}}<extra></extra>",
        ))

    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text="<b>Filing Activity Timeline</b>", x=0.01, font=dict(size=14)),
        barmode="stack",
        xaxis_title="Date",
        yaxis_title="Number of Filings",
        height=300,
    )
    return fig

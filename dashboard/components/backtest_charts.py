"""
dashboard/components/backtest_charts.py

Plotly figure builders for the Backtesting tab.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.components.charts import THEME, _LAYOUT_BASE, _MONO

_EXIT_COLORS = {
    "STOP_LOSS":           "#ff3d5a",
    "MAX_HOLD":            "#f0a31a",
    "INSIDER_SELL":        "#17d890",
    "INSIDER_SELL_PARTIAL": "#5b8fff",
    "OPEN":                "#888",
}


# ---------------------------------------------------------------------------
# Equity curve + drawdown (2 subplots stacked)
# ---------------------------------------------------------------------------

def equity_curve_fig(
    equity_series: pd.Series,
    spy_series: pd.Series,
    starting_capital: float = 100_000.0,
) -> go.Figure:
    """
    Top panel  : portfolio equity line vs SPY (both normalised to 100)
    Bottom panel: drawdown area (shaded red)
    """
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.72, 0.28],
        shared_xaxes=True,
        vertical_spacing=0.04,
    )

    if equity_series.empty:
        fig.update_layout(**_LAYOUT_BASE, height=420, title="No data — run the backtest first")
        return fig

    # Normalise both to 100 from the same start point
    eq_norm  = equity_series / starting_capital * 100
    spy_norm = (spy_series / starting_capital * 100) if not spy_series.empty else pd.Series(dtype=float)

    # Portfolio line
    fig.add_trace(go.Scatter(
        x=list(equity_series.index),
        y=list(eq_norm),
        mode="lines",
        name="Portfolio",
        line=dict(color=THEME["buy_col"], width=2),
        hovertemplate="%{x}<br>Portfolio: %{y:.1f}<extra></extra>",
    ), row=1, col=1)

    # SPY benchmark
    if not spy_norm.empty:
        fig.add_trace(go.Scatter(
            x=list(spy_norm.index),
            y=list(spy_norm),
            mode="lines",
            name="SPY (benchmark)",
            line=dict(color=THEME["line_col"], width=1.5, dash="dot"),
            hovertemplate="%{x}<br>SPY: %{y:.1f}<extra></extra>",
        ), row=1, col=1)

    # Drawdown
    roll_max = equity_series.cummax()
    drawdown = (equity_series - roll_max) / roll_max * 100  # negative pct

    fig.add_trace(go.Scatter(
        x=list(drawdown.index),
        y=list(drawdown),
        mode="lines",
        name="Drawdown",
        fill="tozeroy",
        fillcolor="rgba(255,61,90,0.18)",
        line=dict(color=THEME["sell_col"], width=1),
        hovertemplate="%{x}<br>DD: %{y:.1f}%<extra></extra>",
    ), row=2, col=1)

    layout = {**_LAYOUT_BASE,
              "height": 480,
              "title": dict(text="Equity Curve vs SPY", font=dict(size=14, color=THEME["text"])),
              "yaxis":  dict(**_LAYOUT_BASE["yaxis"], title="Indexed (100 = start)", ticksuffix=""),
              "yaxis2": dict(**_LAYOUT_BASE["yaxis"], title="Drawdown %", ticksuffix="%"),
              "xaxis2": dict(**_LAYOUT_BASE["xaxis"]),
              }
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Monthly returns heatmap
# ---------------------------------------------------------------------------

def monthly_heatmap_fig(equity_series: pd.Series) -> go.Figure:
    """Calendar heatmap — rows = years, columns = months, colour = monthly return %."""
    fig = go.Figure()

    if equity_series.empty:
        fig.update_layout(**_LAYOUT_BASE, height=280, title="No data")
        return fig

    df = equity_series.to_frame("value")
    df.index = pd.to_datetime(df.index)
    monthly = df["value"].resample("ME").last().pct_change() * 100
    monthly = monthly.dropna()

    if monthly.empty:
        fig.update_layout(**_LAYOUT_BASE, height=280, title="Insufficient data for heatmap")
        return fig

    years  = sorted(monthly.index.year.unique())
    months = list(range(1, 13))
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    z    = []
    text = []
    for yr in years:
        row   = []
        t_row = []
        for mo in months:
            idx = pd.Timestamp(year=yr, month=mo, day=1)
            # find the monthly return for this month
            matches = [v for d, v in monthly.items() if d.year == yr and d.month == mo]
            val = matches[0] if matches else np.nan
            row.append(val)
            t_row.append(f"{val:+.1f}%" if not np.isnan(val) else "")
        z.append(row)
        text.append(t_row)

    colorscale = [
        [0.0,   "#7f1d1d"],
        [0.35,  "#ff3d5a"],
        [0.48,  "#1c1e30"],
        [0.52,  "#1c1e30"],
        [0.65,  "#17d890"],
        [1.0,   "#064e3b"],
    ]

    abs_max = max(abs(v) for row in z for v in row if not np.isnan(v)) or 5

    fig.add_trace(go.Heatmap(
        z=z,
        x=month_labels,
        y=[str(yr) for yr in years],
        text=text,
        texttemplate="%{text}",
        textfont=dict(size=10, family=_MONO),
        colorscale=colorscale,
        zmid=0,
        zmin=-abs_max,
        zmax=abs_max,
        showscale=True,
        colorbar=dict(
            title=dict(text="Return %", font=dict(size=9, family=_MONO, color=THEME["text"])),
            tickfont=dict(size=9, family=_MONO, color=THEME["text"]),
            bgcolor=THEME["paper"],
            bordercolor=THEME["grid"],
        ),
        hovertemplate="<b>%{y} %{x}</b><br>Return: %{text}<extra></extra>",
    ))

    fig.update_layout(**{
        **_LAYOUT_BASE,
        "height": max(200, 40 * len(years) + 80),
        "title": dict(text="Monthly Returns", font=dict(size=14, color=THEME["text"])),
        "xaxis": dict(**_LAYOUT_BASE["xaxis"], side="top"),
        "yaxis": dict(**_LAYOUT_BASE["yaxis"], autorange="reversed"),
        "margin": dict(l=55, r=80, t=70, b=20),
    })
    return fig


# ---------------------------------------------------------------------------
# Trade returns histogram
# ---------------------------------------------------------------------------

def trade_histogram_fig(trades_df: pd.DataFrame) -> go.Figure:
    """Histogram of per-trade return_pct with mean line."""
    fig = go.Figure()

    if trades_df.empty or "return_pct" not in trades_df.columns:
        fig.update_layout(**_LAYOUT_BASE, height=300, title="No trades")
        return fig

    rets = trades_df["return_pct"].dropna()
    mean_val = float(rets.mean())

    fig.add_trace(go.Histogram(
        x=list(rets),
        nbinsx=40,
        name="Trade returns",
        marker_color=[THEME["buy_col"] if r >= 0 else THEME["sell_col"] for r in rets],
        opacity=0.8,
        hovertemplate="Return: %{x:.1f}%<br>Count: %{y}<extra></extra>",
    ))

    fig.add_vline(
        x=mean_val,
        line_dash="dash",
        line_color=THEME["award_col"],
        annotation_text=f"Mean: {mean_val:+.1f}%",
        annotation_font=dict(color=THEME["award_col"], size=10, family=_MONO),
    )

    fig.add_vline(x=0, line_color=THEME["grid"], line_width=1)

    fig.update_layout(**{
        **_LAYOUT_BASE,
        "height": 300,
        "title": dict(text="Trade Returns Distribution", font=dict(size=14, color=THEME["text"])),
        "xaxis": dict(**_LAYOUT_BASE["xaxis"], title="Return (%)", ticksuffix="%"),
        "yaxis": dict(**_LAYOUT_BASE["yaxis"], title="# Trades"),
        "bargap": 0.05,
        "showlegend": False,
    })
    return fig


# ---------------------------------------------------------------------------
# Trade scatter (holding period vs return)
# ---------------------------------------------------------------------------

def trade_scatter_fig(trades_df: pd.DataFrame) -> go.Figure:
    """Scatter: x = holding days, y = return %, colour = exit reason, size = position USD."""
    fig = go.Figure()

    if trades_df.empty:
        fig.update_layout(**_LAYOUT_BASE, height=340, title="No trades")
        return fig

    for reason, color in _EXIT_COLORS.items():
        subset = trades_df[trades_df["exit_reason"] == reason] if "exit_reason" in trades_df.columns else pd.DataFrame()
        if subset.empty:
            continue

        size_col = subset["position_usd"].fillna(5000) if "position_usd" in subset.columns else pd.Series([5000] * len(subset))
        marker_sizes = (size_col / size_col.max() * 20 + 5).clip(5, 25)

        custom = list(zip(
            subset.get("ticker", [""]*len(subset)),
            subset.get("insider_name", [""]*len(subset)),
            subset.get("flag_type",    [""]*len(subset)),
            subset.get("severity",     [""]*len(subset)),
        ))

        fig.add_trace(go.Scatter(
            x=list(subset["holding_days"] if "holding_days" in subset.columns else [0]*len(subset)),
            y=list(subset["return_pct"]),
            mode="markers",
            name=reason.replace("_", " ").title(),
            marker=dict(
                color=color,
                size=list(marker_sizes),
                opacity=0.75,
                line=dict(color="rgba(0,0,0,0.3)", width=0.5),
            ),
            customdata=custom,
            hovertemplate=(
                "<b>%{customdata[0]}</b> — %{customdata[1]}<br>"
                "Return: %{y:+.1f}%  |  Hold: %{x}d<br>"
                "Flag: %{customdata[2]}  Severity: %{customdata[3]}"
                "<extra>%{fullData.name}</extra>"
            ),
        ))

    fig.add_hline(y=0, line_color=THEME["grid"], line_width=1)

    fig.update_layout(**{
        **_LAYOUT_BASE,
        "height": 340,
        "title": dict(text="Trades: Holding Period vs Return", font=dict(size=14, color=THEME["text"])),
        "xaxis": dict(**_LAYOUT_BASE["xaxis"], title="Holding period (days)"),
        "yaxis": dict(**_LAYOUT_BASE["yaxis"], title="Return (%)", ticksuffix="%"),
        "legend": dict(**_LAYOUT_BASE["legend"]),
    })
    return fig

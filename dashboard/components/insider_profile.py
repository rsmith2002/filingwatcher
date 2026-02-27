"""
dashboard/components/insider_profile.py

Builds the dynamic content for the Insider Profile tab.
Called from the update_insider_tab callback with pre-fetched DataFrames.
"""

from __future__ import annotations

import pandas as pd
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from dashboard.components.charts import THEME, _MONO, price_with_transactions
from dashboard.components.tables import df_to_records
from ingestion.prices import get_price_series


# ---------------------------------------------------------------------------
# AG Grid column definitions for the per-ticker summary table
# ---------------------------------------------------------------------------

_PCT_FMT    = {"function": "params.value != null ? (params.value >= 0 ? '+' : '') + params.value.toFixed(1) + '%' : '—'"}
_PRICE_FMT  = {"function": "params.value != null ? '$' + params.value.toFixed(2) : '—'"}
_MONEY_FMT  = {"function": "params.value != null ? '$' + params.value.toLocaleString('en-US', {maximumFractionDigits:0}) : '—'"}
_SHARES_FMT = {"function": "params.value != null ? params.value.toLocaleString('en-US', {maximumFractionDigits:0}) : '—'"}


def _pct_style(field: str) -> dict:
    return {
        "function": (
            f"params.data && params.data['{field}'] != null "
            f"? (params.data['{field}'] >= 0 "
            f"  ? {{'color':'#17d890','fontWeight':'600'}} "
            f"  : {{'color':'#ff3d5a','fontWeight':'600'}}) "
            f": {{}}"
        )
    }


PROFILE_GRID_COLS = [
    {"field": "ticker",                  "headerName": "Ticker",       "width": 85,  "pinned": "left"},
    {"field": "company_name",            "headerName": "Company",      "width": 185},
    {"field": "officer_title",           "headerName": "Role",         "width": 170},
    {"field": "first_txn_date",          "headerName": "Entry Date",   "width": 108},
    {"field": "entry_price",             "headerName": "Entry $",      "width": 88,
     "valueFormatter": _PRICE_FMT},
    {"field": "current_price",           "headerName": "Current $",    "width": 92,
     "valueFormatter": _PRICE_FMT},
    {"field": "return_col",              "headerName": "Return (win)", "width": 108,
     "valueFormatter": _PCT_FMT, "cellStyle": _pct_style("return_col")},
    {"field": "stock_pct_since_entry",   "headerName": "Since Entry",  "width": 105,
     "valueFormatter": _PCT_FMT, "cellStyle": _pct_style("stock_pct_since_entry")},
    {"field": "last_reported_shares",    "headerName": "Shares Held",  "width": 112,
     "valueFormatter": _SHARES_FMT},
    {"field": "current_position_value",  "headerName": "Value",        "width": 100,
     "valueFormatter": _MONEY_FMT},
    {"field": "n_open_mkt_buys",         "headerName": "Buys",         "width": 62},
    {"field": "n_open_mkt_sells",        "headerName": "Sells",        "width": 62},
    {"field": "open_mkt_wacb",           "headerName": "WACB",         "width": 88,
     "valueFormatter": _PRICE_FMT},
    {"field": "open_mkt_unrealized_pct", "headerName": "Unreal. P&L",  "width": 108,
     "valueFormatter": _PCT_FMT, "cellStyle": _pct_style("open_mkt_unrealized_pct")},
    {"field": "open_mkt_total_cost",     "headerName": "Cost Basis",   "width": 112,
     "valueFormatter": _MONEY_FMT},
    {"field": "open_mkt_total_proceeds", "headerName": "Proceeds",     "width": 108,
     "valueFormatter": _MONEY_FMT},
    {"field": "realized_pct",            "headerName": "Realized %",   "width": 100,
     "valueFormatter": _PCT_FMT, "cellStyle": _pct_style("realized_pct")},
]

_PROFILE_DEFAULT_COL = {
    "resizable": True,
    "sortable": True,
    "filter": True,
    "suppressMovable": False,
    "floatingFilter": False,
    "cellStyle": {"fontFamily": _MONO, "fontSize": "12px"},
}


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _stat_card(value: str, label: str, accent: str = "") -> dbc.Col:
    val_color = {
        "buy":   THEME["buy_col"],
        "sell":  THEME["sell_col"],
        "amber": THEME["award_col"],
    }.get(accent, "#e8e0d2")
    return dbc.Col(
        html.Div([
            html.Div(value, style={
                "fontSize": "19px", "fontWeight": "700",
                "color": val_color, "fontFamily": _MONO,
                "lineHeight": "1.1",
            }),
            html.Div(label, style={
                "fontSize": "9px", "color": THEME["award_col"],
                "fontFamily": _MONO, "textTransform": "uppercase",
                "letterSpacing": "0.14em", "marginTop": "5px", "opacity": "0.75",
            }),
        ], style={
            "backgroundColor": "#0d0e1b",
            "border": "1px solid #1c1e30",
            "borderRadius": "4px",
            "padding": "13px 16px",
            "minWidth": "95px",
        }),
        width="auto",
        className="pe-2",
    )


def _section_label(text: str) -> html.Div:
    return html.Div(text, style={
        "fontSize": "10px", "color": THEME["award_col"],
        "fontFamily": _MONO, "fontWeight": "600",
        "letterSpacing": "0.14em", "textTransform": "uppercase",
        "marginBottom": "10px", "marginTop": "4px",
    })


# ---------------------------------------------------------------------------
# Per-ticker mini stat strip (shown above each price chart)
# ---------------------------------------------------------------------------

def _ticker_stat_strip(row: pd.Series, window: str) -> html.Div:
    col = f"pct_{window}"

    def _val(key, fmt=""):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        v = float(v)
        if fmt == "$":      return f"${v:.2f}"
        if fmt == "$k":     return f"${v/1e6:.1f}M" if v >= 1e6 else f"${v:,.0f}"
        if fmt == "%":      return f"{v:+.1f}%"
        if fmt == "n":      return f"{v:,.0f}"
        if fmt == "int":    return str(int(v))
        return str(v)

    pairs = [
        ("Entry",       _val("entry_price",              "$")),
        ("Current",     _val("current_price",             "$")),
        (f"Ret {window}", _val(col,                        "%")),
        ("Value",       _val("current_position_value",    "$k")),
        ("Shares",      _val("last_reported_shares",      "n")),
        ("Buys",        _val("n_open_mkt_buys",           "int")),
        ("WACB",        _val("open_mkt_wacb",             "$")),
        ("Unreal.",     _val("open_mkt_unrealized_pct",   "%")),
    ]

    items = []
    for lbl, val in pairs:
        is_pct = lbl in (f"Ret {window}", "Unreal.")
        try:
            num = float(
                val.replace("$", "").replace("%", "").replace("+", "")
                   .replace(",", "").replace("M", "")
            ) if val != "—" else None
        except (ValueError, AttributeError):
            num = None
        vc = (
            THEME["buy_col"]  if (is_pct and num is not None and num > 0) else
            THEME["sell_col"] if (is_pct and num is not None and num < 0) else
            "#c6bead"
        )
        items.append(html.Span([
            html.Span(lbl + " ", style={
                "color": THEME["award_col"], "fontSize": "9px",
                "letterSpacing": "0.10em", "fontFamily": _MONO,
                "textTransform": "uppercase",
            }),
            html.Span(val, style={
                "color": vc, "fontSize": "12px",
                "fontWeight": "600", "fontFamily": _MONO,
            }),
        ], style={"marginRight": "20px", "display": "inline-block", "whiteSpace": "nowrap"}))

    return html.Div(items, style={
        "display": "flex", "flexWrap": "wrap",
        "marginBottom": "8px", "gap": "2px",
    })


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

def _empty_state() -> html.Div:
    return html.Div([
        html.Div("👤", style={
            "fontSize": "52px", "opacity": "0.12",
            "textAlign": "center", "marginTop": "80px",
        }),
        html.Div(
            "Click any row in Activity or Leaderboard to view an insider's full profile",
            style={
                "textAlign": "center", "color": "#5a5347",
                "fontFamily": _MONO, "fontSize": "12px", "marginTop": "14px",
            },
        ),
    ])


# ---------------------------------------------------------------------------
# Main layout builder
# ---------------------------------------------------------------------------

def build_insider_content(
    insider_name: str,
    window: str,
    filings_df: pd.DataFrame,
    analytics_df: pd.DataFrame,
) -> list:
    """
    Returns a list of Dash components for the full insider profile.
    filings_df / analytics_df contain ALL records for this insider (all tickers).
    """
    col = f"pct_{window}"
    tickers = sorted(filings_df["ticker"].unique().tolist()) if not filings_df.empty else []

    # Role / title
    role = "Insider"
    if not analytics_df.empty and "officer_title" in analytics_df.columns:
        titles = analytics_df["officer_title"].dropna().unique()
        role = " · ".join(t for t in titles if t) or "Insider"

    # ── Summary stats ────────────────────────────────────────────────────────
    def _safe_sum(key):
        if analytics_df.empty or key not in analytics_df.columns:
            return None
        v = analytics_df[key].sum(min_count=1)
        return None if pd.isna(v) else float(v)

    total_cost    = _safe_sum("open_mkt_total_cost")
    total_value   = _safe_sum("current_position_value")
    total_pnl_usd = _safe_sum("open_mkt_unrealized_usd")
    total_buys    = _safe_sum("n_open_mkt_buys")
    total_sells   = _safe_sum("n_open_mkt_sells")

    total_pnl_pct = None
    if total_cost and total_cost > 0 and total_pnl_usd is not None:
        total_pnl_pct = (total_pnl_usd / total_cost) * 100

    pnl_accent = "buy" if (total_pnl_pct or 0) >= 0 else "sell"

    def _fmt_money(v):
        if v is None:
            return "—"
        return f"${v / 1e6:.2f}M" if v >= 1e6 else f"${v:,.0f}"

    stats_row = dbc.Row([
        _stat_card(str(len(tickers)), "Tickers"),
        _stat_card(_fmt_money(total_cost),  "Cost Basis"),
        _stat_card(_fmt_money(total_value), "Position Value"),
        _stat_card(
            f"{total_pnl_pct:+.1f}%" if total_pnl_pct is not None else "—",
            "Unrealized P&L %", pnl_accent,
        ),
        _stat_card(
            f"${total_pnl_usd:+,.0f}" if total_pnl_usd is not None else "—",
            "Unrealized USD", pnl_accent,
        ),
        _stat_card(str(int(total_buys))  if total_buys  is not None else "0", "Open-Mkt Buys"),
        _stat_card(str(int(total_sells)) if total_sells is not None else "0", "Open-Mkt Sells"),
    ], className="g-2 mb-3")

    # ── Header ───────────────────────────────────────────────────────────────
    header = html.Div([
        html.H4(insider_name, className="mb-0", style={
            "color": "#e8e0d2", "fontWeight": "700",
            "fontFamily": "'Syne', sans-serif", "fontSize": "22px",
        }),
        html.Div(role, style={
            "color": THEME["award_col"], "fontSize": "11px",
            "fontFamily": _MONO, "marginTop": "3px",
        }),
        html.Div(" · ".join(tickers) if tickers else "No ticker data", style={
            "color": "#5a5347", "fontSize": "10px",
            "fontFamily": _MONO, "letterSpacing": "0.10em", "marginTop": "3px",
        }),
    ], className="mb-3")

    # ── Per-ticker analytics table ────────────────────────────────────────────
    if not analytics_df.empty:
        table_df = analytics_df.copy()
        table_df["return_col"] = table_df[col] if col in table_df.columns else None
        table_records = df_to_records(table_df)
    else:
        table_records = []

    analytics_section = html.Div([
        _section_label("Per-Ticker Performance"),
        dag.AgGrid(
            id="grid-insider-profile",
            columnDefs=PROFILE_GRID_COLS,
            rowData=table_records,
            defaultColDef=_PROFILE_DEFAULT_COL,
            dashGridOptions={
                "animateRows": True,
                "pagination": False,
                "domLayout": "autoHeight",
                "headerHeight": 36,
            },
            className="ag-theme-alpine-dark",
            style={"width": "100%"},
        ),
    ], className="mb-4")

    # ── Per-ticker price charts ───────────────────────────────────────────────
    chart_divs = []
    for ticker in tickers:
        ticker_filings = filings_df[filings_df["ticker"] == ticker].copy()

        prices_df = pd.DataFrame()
        try:
            ps = get_price_series(ticker)
            if not ps.empty:
                prices_df = ps.rename("close").to_frame()
        except Exception:
            pass

        fig = price_with_transactions(ticker, prices_df, ticker_filings, insider_name)

        stat_strip = None
        if not analytics_df.empty:
            ta = analytics_df[analytics_df["ticker"] == ticker]
            if not ta.empty:
                stat_strip = _ticker_stat_strip(ta.iloc[0], window)

        chart_divs.append(html.Div([
            stat_strip,
            dcc.Graph(figure=fig, config={"displayModeBar": True}),
        ], style={
            "backgroundColor": "#0d0e1b",
            "border": "1px solid #1c1e30",
            "borderRadius": "4px",
            "padding": "14px 16px 10px 16px",
            "marginBottom": "16px",
        }))

    if not chart_divs:
        chart_divs = [dbc.Alert(
            "No transaction history found for this insider.",
            color="secondary",
        )]

    charts_section = html.Div([
        _section_label("Price History & Transactions"),
        *chart_divs,
    ])

    return [
        header,
        stats_row,
        html.Hr(style={"borderColor": "#1c1e30", "margin": "16px 0"}),
        analytics_section,
        html.Hr(style={"borderColor": "#1c1e30", "margin": "16px 0"}),
        charts_section,
    ]

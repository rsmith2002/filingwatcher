"""
dashboard/layout.py

Full page layout for the CeoWatcher Dash app.
"""

from __future__ import annotations

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

from config import COMPANIES, RETURN_WINDOWS
from dashboard.components.tables import (
    ACTIVITY_COLUMNS, ACTIVITY_ROW_STYLE,
    FLAGS_COLUMNS, LEADERBOARD_COLUMNS,
    _DEFAULT_COL,
)

_TICKER_OPTIONS = [{"label": f"{t} — {n}", "value": t} for t, n in COMPANIES]
_TICKER_VALUES  = [t for t, _ in COMPANIES]
_WINDOW_OPTIONS = [{"label": k, "value": k} for k in RETURN_WINDOWS]

SIDEBAR_WIDTH = "260px"

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _sidebar() -> dbc.Col:
    return dbc.Col(
        html.Div([
            # ── Logo / title ──────────────────────────────────────────────
            html.Div([
                html.H4("CEOWATCHER", className="mb-0",
                        style={"color": "#f0a31a", "fontWeight": "800",
                               "letterSpacing": "0.10em", "fontSize": "20px",
                               "fontFamily": "'Syne', sans-serif"}),
                html.Small("INSIDER INTELLIGENCE",
                           style={"color": "#5a5347", "fontSize": "8px",
                                  "letterSpacing": "0.22em", "display": "block",
                                  "marginTop": "3px",
                                  "fontFamily": "'JetBrains Mono', monospace"}),
            ], className="mb-4"),

            # ── Last updated badge ────────────────────────────────────────
            html.Div(id="last-updated-badge", className="mb-4"),

            # ── Tickers ───────────────────────────────────────────────────
            html.Label("Tickers", className="sidebar-label"),
            dcc.Dropdown(
                id="filter-tickers",
                options=_TICKER_OPTIONS,
                value=_TICKER_VALUES,
                multi=True,
                clearable=False,
                className="mb-3 dark-dropdown",
            ),

            # ── Filing type ───────────────────────────────────────────────
            html.Label("Filing Type", className="sidebar-label"),
            dbc.Checklist(
                id="filter-forms",
                options=[
                    {"label": "Form 3 (Initial)",    "value": "3"},
                    {"label": "Form 4 (Transactions)","value": "4"},
                    {"label": "Form 5 (Annual)",      "value": "5"},
                    {"label": "13D / 13G (Stakes)",   "value": "13"},
                ],
                value=["3", "4", "5"],
                switch=True,
                className="mb-3",
            ),

            # ── Role filter ───────────────────────────────────────────────
            html.Label("Role", className="sidebar-label"),
            dbc.Checklist(
                id="filter-roles",
                options=[
                    {"label": "Directors",         "value": "director"},
                    {"label": "Officers / CEOs",   "value": "officer"},
                    {"label": "10%+ Owners",       "value": "ten_pct"},
                ],
                value=["director", "officer", "ten_pct"],
                switch=True,
                className="mb-3",
            ),

            # ── Insider search ────────────────────────────────────────────
            html.Label("Search Insider", className="sidebar-label"),
            dbc.Input(
                id="filter-insider-search",
                placeholder="Type name …",
                type="text",
                debounce=True,
                className="mb-3",
            ),

            # ── Return window ─────────────────────────────────────────────
            html.Label("Return Window", className="sidebar-label"),
            dbc.RadioItems(
                id="filter-window",
                options=_WINDOW_OPTIONS,
                value="3m",
                inline=False,
                className="mb-3",
            ),

            # ── Min return filter ─────────────────────────────────────────
            html.Label("Min Return in Window", className="sidebar-label"),
            dbc.InputGroup([
                dbc.Input(id="filter-min-return", type="number", value=0, min=-100, max=1000,
                          style={"backgroundColor": "#16213e", "color": "#e0e0e0",
                                 "border": "1px solid #2a2a4a"}),
                dbc.InputGroupText("%", style={"backgroundColor": "#2a2a4a",
                                               "color": "#e0e0e0", "border": "none"}),
            ], className="mb-3"),

            # ── Top X% filter ─────────────────────────────────────────────
            html.Label("Show Top X% by Return", className="sidebar-label"),
            dcc.Slider(
                id="filter-top-pct",
                min=10, max=100, step=10, value=100,
                marks={v: f"{v}%" for v in range(10, 110, 10)},
                tooltip={"placement": "bottom"},
                className="mb-4",
            ),

            html.Hr(style={"borderColor": "#2a2a4a"}),
            dbc.Button("Reset Filters", id="btn-reset-filters",
                       color="secondary", size="sm", outline=True,
                       className="w-100"),
        ],
        style={
            "width": SIDEBAR_WIDTH,
            "minWidth": SIDEBAR_WIDTH,
            "padding": "24px 18px 24px 20px",
            "backgroundColor": "#0d0e1b",
            "height": "100vh",
            "overflowY": "auto",
            "borderRight": "1px solid #1c1e30",
            "borderLeft": "3px solid #f0a31a",
            "position": "sticky",
            "top": 0,
        }),
        width="auto",
    )


# ---------------------------------------------------------------------------
# Main content tabs
# ---------------------------------------------------------------------------

def _tab_activity() -> dcc.Tab:
    return dcc.Tab(label="📋 Activity Feed", value="tab-activity",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            dbc.Row([
                dbc.Col(dcc.Graph(id="chart-activity-timeline", config={"displayModeBar": False}),
                        width=12),
            ], className="mt-3"),
            dbc.Row([
                dbc.Col(
                    dag.AgGrid(
                        id="grid-activity",
                        columnDefs=ACTIVITY_COLUMNS,
                        rowData=[],
                        defaultColDef=_DEFAULT_COL,
                        dashGridOptions={
                            "rowSelection": "single",
                            "animateRows": True,
                            "getRowStyle": ACTIVITY_ROW_STYLE,
                            "pagination": True,
                            "paginationPageSize": 50,
                        },
                        style={"height": "500px"},
                        className="ag-theme-alpine-dark",
                    ),
                    width=12,
                ),
            ], className="mt-2"),
        ]
    )


def _tab_leaderboard() -> dcc.Tab:
    return dcc.Tab(label="🏆 Leaderboard", value="tab-leaderboard",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            dbc.Row([
                dbc.Col(dcc.Graph(id="chart-return-scatter", config={"displayModeBar": False}),
                        width=12),
            ], className="mt-3"),
            dbc.Row([
                dbc.Col(
                    dag.AgGrid(
                        id="grid-leaderboard",
                        columnDefs=LEADERBOARD_COLUMNS,
                        rowData=[],
                        defaultColDef=_DEFAULT_COL,
                        dashGridOptions={
                            "rowSelection": "single",
                            "animateRows": True,
                            "pagination": True,
                            "paginationPageSize": 25,
                        },
                        style={"height": "500px"},
                        className="ag-theme-alpine-dark",
                    ),
                    width=12,
                ),
            ], className="mt-2"),
        ]
    )


def _tab_charts() -> dcc.Tab:
    return dcc.Tab(label="📈 Price Charts", value="tab-charts",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            dbc.Row([
                dbc.Col([
                    html.Label("Select Ticker", style={"color": "#888", "fontSize": "12px"}),
                    dcc.Dropdown(
                        id="chart-ticker-select",
                        options=[{"label": t, "value": t} for t, _ in COMPANIES],
                        value=COMPANIES[0][0],
                        clearable=False,
                        className="dark-dropdown",
                        style={"width": "200px"},
                    ),
                ], width="auto"),
                dbc.Col([
                    html.Label("\u00a0", style={"fontSize": "12px"}),
                    dbc.Button(
                        "Show All Insiders", id="btn-clear-insider",
                        size="sm", outline=True, color="secondary",
                        style={"marginTop": "2px"},
                    ),
                ], width="auto"),
            ], className="mt-3 mb-2", align="end"),
            dbc.Row([
                dbc.Col(dcc.Graph(id="chart-price-transactions",
                                  config={"displayModeBar": True}),
                        width=12),
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id="chart-unrealized-pnl",
                                  config={"displayModeBar": False}),
                        width=6),
                dbc.Col(dcc.Graph(id="chart-position-values",
                                  config={"displayModeBar": False}),
                        width=6),
            ], className="mt-2"),
        ]
    )


def _tab_insider() -> dcc.Tab:
    return dcc.Tab(label="👤 Insider", value="tab-insider",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            html.Div(id="insider-profile-content",
                     style={"padding": "16px 0 32px 0"}),
        ]
    )


def _tab_flags() -> dcc.Tab:
    return dcc.Tab(label="🚩 Flags", value="tab-flags",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            dbc.Row([
                dbc.Col(
                    html.Div(id="flags-summary-cards", className="d-flex flex-wrap gap-3 mt-3"),
                    width=12,
                ),
            ]),
            dbc.Row([
                dbc.Col(
                    dag.AgGrid(
                        id="grid-flags",
                        columnDefs=FLAGS_COLUMNS,
                        rowData=[],
                        defaultColDef=_DEFAULT_COL,
                        dashGridOptions={
                            "rowSelection": "single",
                            "animateRows": True,
                            "pagination": True,
                            "paginationPageSize": 25,
                        },
                        style={"height": "500px"},
                        className="ag-theme-alpine-dark",
                    ),
                    width=12,
                ),
            ], className="mt-3"),
        ]
    )


def _tab_backtesting() -> dcc.Tab:
    _input_style = {
        "backgroundColor": "#0d0e1b", "border": "1px solid #1c1e30",
        "borderRadius": "4px", "color": "#c6bead",
        "fontFamily": "'JetBrains Mono', monospace", "fontSize": "12px",
        "padding": "6px 10px", "width": "100%",
    }
    _label_style = {
        "color": "#f0a31a", "fontSize": "9px",
        "fontFamily": "'JetBrains Mono', monospace",
        "textTransform": "uppercase", "letterSpacing": "0.12em",
        "display": "block", "marginBottom": "4px",
    }

    def _cfg(label, id_, default, step=None):
        return dbc.Col([
            html.Label(label, style=_label_style),
            dcc.Input(id=id_, type="number", value=default,
                      step=step or 1, style=_input_style,
                      debounce=True),
        ], width="auto", className="pe-3")

    return dcc.Tab(label="📊 Backtest", value="tab-backtesting",
                   className="custom-tab", selected_className="custom-tab-selected",
        children=[
            # ── Config row ────────────────────────────────────────────────
            dbc.Row([
                _cfg("Capital ($)",       "bt-capital",    100000, 1000),
                _cfg("Base % / unit",     "bt-base-pct",   5,      0.5),
                _cfg("Max hold (days)",   "bt-max-hold",   90,     1),
                _cfg("Stop loss (%)",     "bt-stop-loss",  10,     0.5),
                _cfg("Slippage (%)",      "bt-slippage",   0.10,   0.01),
                _cfg("Risk-free rate (%)", "bt-rfr",       5,      0.25),
                dbc.Col([
                    html.Label("\u00a0", style=_label_style),
                    dbc.Button("▶  Run Backtest", id="btn-run-backtest",
                               color="warning", size="sm",
                               style={"fontFamily": "'JetBrains Mono', monospace",
                                      "fontSize": "12px", "fontWeight": "600"}),
                ], width="auto", className="align-self-end pb-1"),
            ], className="mt-3 mb-3 align-items-end"),

            # ── KPI stat cards ────────────────────────────────────────────
            html.Div(id="bt-stat-cards",
                     className="d-flex flex-wrap gap-2 mb-3",
                     style={"minHeight": "60px"}),

            # ── Equity curve ──────────────────────────────────────────────
            dcc.Loading(
                dcc.Graph(id="bt-chart-equity",
                          config={"displayModeBar": True},
                          style={"minHeight": "480px"}),
                color=THEME["award_col"],
            ),

            # ── Monthly heatmap + Histogram ───────────────────────────────
            dbc.Row([
                dbc.Col(dcc.Graph(id="bt-chart-heatmap",
                                  config={"displayModeBar": False}),
                        width=6),
                dbc.Col(dcc.Graph(id="bt-chart-histogram",
                                  config={"displayModeBar": False}),
                        width=6),
            ], className="mt-2"),

            # ── Trades scatter ────────────────────────────────────────────
            dcc.Graph(id="bt-chart-scatter",
                      config={"displayModeBar": False},
                      className="mt-2"),

            # ── Trade log grid ────────────────────────────────────────────
            html.Div([
                html.Div("Trade Log", style={
                    "fontSize": "10px", "color": "#f0a31a",
                    "fontFamily": "'JetBrains Mono', monospace",
                    "fontWeight": "600", "letterSpacing": "0.14em",
                    "textTransform": "uppercase", "marginBottom": "8px",
                }),
                dag.AgGrid(
                    id="grid-trades",
                    columnDefs=[
                        {"field": "ticker",       "headerName": "Ticker",     "width": 80,  "pinned": "left"},
                        {"field": "insider_name", "headerName": "Insider",    "width": 200},
                        {"field": "flag_type",    "headerName": "Flag",       "width": 150},
                        {"field": "severity",     "headerName": "Sev",        "width": 70},
                        {"field": "entry_date",   "headerName": "Entry",      "width": 100},
                        {"field": "exit_date",    "headerName": "Exit",       "width": 100},
                        {"field": "holding_days", "headerName": "Days",       "width": 65},
                        {"field": "exit_reason",  "headerName": "Exit Reason","width": 140},
                        {"field": "entry_price",  "headerName": "Entry $",    "width": 90,
                         "valueFormatter": {"function": "params.value != null ? '$' + params.value.toFixed(2) : '—'"}},
                        {"field": "exit_price",   "headerName": "Exit $",     "width": 90,
                         "valueFormatter": {"function": "params.value != null ? '$' + params.value.toFixed(2) : '—'"}},
                        {"field": "return_pct",   "headerName": "Return %",   "width": 95,
                         "valueFormatter": {"function": "params.value != null ? (params.value >= 0 ? '+' : '') + params.value.toFixed(1) + '%' : '—'"},
                         "cellStyle": {"function": "params.value != null ? (params.value >= 0 ? {'color':'#17d890','fontWeight':'600'} : {'color':'#ff3d5a','fontWeight':'600'}) : {}"}},
                        {"field": "return_usd",   "headerName": "Return $",   "width": 100,
                         "valueFormatter": {"function": "params.value != null ? (params.value >= 0 ? '+$' : '-$') + Math.abs(params.value).toLocaleString('en-US', {maximumFractionDigits:0}) : '—'"}},
                        {"field": "position_usd", "headerName": "Position $", "width": 105,
                         "valueFormatter": {"function": "params.value != null ? '$' + params.value.toLocaleString('en-US', {maximumFractionDigits:0}) : '—'"}},
                    ],
                    rowData=[],
                    defaultColDef={
                        "resizable": True, "sortable": True, "filter": True,
                        "cellStyle": {"fontFamily": "'JetBrains Mono', monospace", "fontSize": "11px"},
                    },
                    dashGridOptions={
                        "animateRows": True,
                        "pagination": True,
                        "paginationPageSize": 50,
                    },
                    style={"height": "520px"},
                    className="ag-theme-alpine-dark",
                ),
            ], className="mt-3 mb-4"),

            # Hidden store for backtest results
            dcc.Store(id="store-backtest-results", data=None),
        ]
    )


# Pull THEME import for the loading spinner colour
from dashboard.components.charts import THEME  # noqa: E402


# ---------------------------------------------------------------------------
# Root layout
# ---------------------------------------------------------------------------

def build_layout() -> html.Div:
    return html.Div([
        # Auto-refresh every 30 minutes
        dcc.Interval(id="auto-refresh", interval=30 * 60 * 1000, n_intervals=0),

        # Store for filtered data shared across callbacks
        dcc.Store(id="store-filtered-filings"),
        dcc.Store(id="store-filtered-analytics"),
        # Selected insider — set by clicking any grid row
        dcc.Store(id="store-selected-insider", data=None),

        dbc.Row([
            # Sidebar
            _sidebar(),

            # Main content
            dbc.Col([
                dcc.Tabs(
                    id="main-tabs",
                    value="tab-activity",
                    children=[
                        _tab_activity(),
                        _tab_leaderboard(),
                        _tab_charts(),
                        _tab_flags(),
                        _tab_insider(),
                        _tab_backtesting(),
                    ],
                    className="custom-tabs",
                ),
            ], style={"padding": "0 24px 24px 24px", "flex": "1"}),
        ],
        style={"display": "flex", "flexWrap": "nowrap", "minHeight": "100vh"},
        className="g-0"),
    ],
    style={"backgroundColor": "#080810", "minHeight": "100vh",
           "fontFamily": "'Syne', system-ui, sans-serif"})

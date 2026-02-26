"""
dashboard/callbacks.py

All Dash interactivity. Every filter change triggers a re-query of the DB
and refreshes only the active tab's components.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from dash import Input, Output, State, callback, html, no_update
from sqlalchemy import and_, or_

from config import COMPANIES, RETURN_WINDOWS
from dashboard.components.charts import (
    activity_timeline, position_values_bar,
    price_with_transactions, return_window_scatter, unrealized_pnl_bar,
)
from dashboard.components.tables import df_to_records
from db.models import Flag, InsiderAnalytics, LargeHolderStake, Section16Filing
from db.session import get_session
from ingestion.prices import get_price_series


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _query_filings(
    tickers: list[str],
    forms: list[str],
    roles: list[str],
    search: str,
) -> pd.DataFrame:
    session = get_session()
    try:
        q = session.query(Section16Filing)
        if tickers:
            q = q.filter(Section16Filing.ticker.in_(tickers))

        # Form filter — map "13" to large-holder table (handled separately)
        form_codes = []
        for f in forms:
            if f == "3":   form_codes.append("3")
            elif f == "4": form_codes.extend(["4", "4/A"])
            elif f == "5": form_codes.extend(["5", "5/A"])
        if form_codes:
            q = q.filter(Section16Filing.filing_form.in_(form_codes))
        else:
            return pd.DataFrame()

        # Role filter — skip entirely when all roles selected (avoids filtering out
        # rows where all flags are False because any() resolved to False in fetcher)
        _ALL_ROLES = {"director", "officer", "ten_pct"}
        if set(roles) != _ALL_ROLES:
            role_filters = []
            if "director" in roles:
                role_filters.append(Section16Filing.is_director == True)
            if "officer" in roles:
                role_filters.append(Section16Filing.is_officer == True)
            if "ten_pct" in roles:
                role_filters.append(Section16Filing.is_ten_pct_owner == True)
            if role_filters:
                q = q.filter(or_(*role_filters))
            else:
                return pd.DataFrame()

        # Name search
        if search and search.strip():
            q = q.filter(Section16Filing.insider_name.ilike(f"%{search.strip()}%"))

        rows = q.order_by(Section16Filing.filing_date.desc()).limit(5000).all()
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([{
            c.key: getattr(r, c.key)
            for c in Section16Filing.__table__.columns
        } for r in rows])
    finally:
        session.close()


def _query_analytics(
    tickers: list[str],
    roles: list[str],
    search: str,
    window: str,
    min_return: float,
    top_pct: int,
) -> pd.DataFrame:
    session = get_session()
    try:
        q = session.query(InsiderAnalytics)
        if tickers:
            q = q.filter(InsiderAnalytics.ticker.in_(tickers))

        _ALL_ROLES = {"director", "officer", "ten_pct"}
        if set(roles) != _ALL_ROLES:
            role_filters = []
            if "director" in roles:
                role_filters.append(InsiderAnalytics.is_director == True)
            if "officer" in roles:
                role_filters.append(InsiderAnalytics.is_officer == True)
            if "ten_pct" in roles:
                role_filters.append(InsiderAnalytics.is_ten_pct_owner == True)
            if role_filters:
                q = q.filter(or_(*role_filters))

        if search and search.strip():
            q = q.filter(InsiderAnalytics.insider_name.ilike(f"%{search.strip()}%"))

        rows = q.all()
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([{
            c.key: getattr(r, c.key)
            for c in InsiderAnalytics.__table__.columns
        } for r in rows])

        # Window return filter
        col = f"pct_{window}"
        if col in df.columns and min_return != 0:
            df = df[df[col].isna() | (df[col] >= min_return)]

        # Top X% by return in window
        if top_pct < 100 and col in df.columns:
            df = df.sort_values(col, ascending=False)
            n_keep = max(1, int(len(df) * top_pct / 100))
            df = df.head(n_keep)

        return df
    finally:
        session.close()


def _query_flags(tickers: list[str]) -> pd.DataFrame:
    session = get_session()
    try:
        q = session.query(Flag).filter_by(is_dismissed=False)
        if tickers:
            q = q.filter(Flag.ticker.in_(tickers))
        rows = q.order_by(Flag.flagged_at.desc()).limit(200).all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([{
            c.key: getattr(r, c.key)
            for c in Flag.__table__.columns
        } for r in rows])
    finally:
        session.close()


def _last_sync_text() -> str:
    from db.models import IngestRun
    session = get_session()
    try:
        run = session.query(IngestRun).order_by(IngestRun.run_at.desc()).first()
        if not run:
            return "Never synced"
        delta = datetime.utcnow() - run.run_at
        h = int(delta.total_seconds() // 3600)
        m = int((delta.total_seconds() % 3600) // 60)
        return f"Last sync: {h}h {m}m ago — {run.status}"
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def register_callbacks(app):

    # ── Last-updated badge ───────────────────────────────────────────────
    @app.callback(
        Output("last-updated-badge", "children"),
        Input("auto-refresh", "n_intervals"),
    )
    def update_badge(_):
        import dash_bootstrap_components as dbc
        text  = _last_sync_text()
        color = "success" if "success" in text else "warning" if "partial" in text else "secondary"
        return dbc.Badge(text, color=color, pill=True,
                         style={"fontSize": "10px", "whiteSpace": "normal"})

    # ── Shared filtered data stores ──────────────────────────────────────
    @app.callback(
        Output("store-filtered-filings",  "data"),
        Output("store-filtered-analytics", "data"),
        Input("filter-tickers",       "value"),
        Input("filter-forms",         "value"),
        Input("filter-roles",         "value"),
        Input("filter-insider-search","value"),
        Input("filter-window",        "value"),
        Input("filter-min-return",    "value"),
        Input("filter-top-pct",       "value"),
        Input("auto-refresh",         "n_intervals"),
    )
    def update_stores(tickers, forms, roles, search, window, min_ret, top_pct, _):
        tickers  = tickers  or []
        forms    = forms    or ["4"]
        roles    = roles    or ["director", "officer", "ten_pct"]
        min_ret  = min_ret  or 0
        top_pct  = top_pct  or 100
        window   = window   or "3m"

        filings_df  = _query_filings(tickers, forms, roles, search)
        analytics_df = _query_analytics(tickers, roles, search, window, min_ret, top_pct)

        return (
            df_to_records(filings_df),
            df_to_records(analytics_df),
        )

    # ── Activity Feed ────────────────────────────────────────────────────
    @app.callback(
        Output("grid-activity",          "rowData"),
        Output("chart-activity-timeline","figure"),
        Input("store-filtered-filings",  "data"),
    )
    def update_activity(records):
        if not records:
            import plotly.graph_objects as go
            from dashboard.components.charts import _LAYOUT_BASE
            empty = go.Figure()
            empty.update_layout(**_LAYOUT_BASE, title="No data — run the pipeline first.")
            return [], empty
        df = pd.DataFrame(records)
        return df_to_records(df), activity_timeline(df)

    # ── Leaderboard ──────────────────────────────────────────────────────
    @app.callback(
        Output("grid-leaderboard",     "rowData"),
        Output("chart-return-scatter", "figure"),
        Input("store-filtered-analytics", "data"),
        Input("filter-window",            "value"),
    )
    def update_leaderboard(records, window):
        window = window or "3m"
        if not records:
            import plotly.graph_objects as go
            from dashboard.components.charts import _LAYOUT_BASE
            empty = go.Figure()
            empty.update_layout(**_LAYOUT_BASE, title="No analytics data yet.")
            return [], empty
        df  = pd.DataFrame(records)
        col = f"pct_{window}"
        if col in df.columns:
            df = df.sort_values(col, ascending=False, na_position="last")
        return df_to_records(df), return_window_scatter(df, window)

    # ── Price Charts ─────────────────────────────────────────────────────
    @app.callback(
        Output("chart-price-transactions", "figure"),
        Output("chart-unrealized-pnl",     "figure"),
        Output("chart-position-values",    "figure"),
        Input("chart-ticker-select",       "value"),
        Input("store-filtered-filings",    "data"),
        Input("store-filtered-analytics",  "data"),
    )
    def update_charts(ticker, filing_records, analytics_records):
        filings_df  = pd.DataFrame(filing_records  or [])
        analytics_df = pd.DataFrame(analytics_records or [])

        # Price series for selected ticker
        prices_df = pd.DataFrame()
        if ticker:
            ps = get_price_series(ticker)
            if not ps.empty:
                prices_df = ps.rename("close").to_frame()

        ticker_filings = (
            filings_df[filings_df["ticker"] == ticker].copy()
            if not filings_df.empty and ticker else pd.DataFrame()
        )

        fig1 = price_with_transactions(ticker or "—", prices_df, ticker_filings)
        fig2 = unrealized_pnl_bar(analytics_df)
        fig3 = position_values_bar(analytics_df)
        return fig1, fig2, fig3

    # ── Flags ────────────────────────────────────────────────────────────
    @app.callback(
        Output("grid-flags",          "rowData"),
        Output("flags-summary-cards", "children"),
        Input("filter-tickers",       "value"),
        Input("auto-refresh",         "n_intervals"),
    )
    def update_flags(tickers, _):
        import dash_bootstrap_components as dbc
        flags_df = _query_flags(tickers or [])
        if flags_df.empty:
            cards = [dbc.Alert("No active flags. Good news or no data yet.",
                               color="secondary")]
            return [], cards

        # Summary cards
        cards = []
        for _, row in flags_df.head(6).iterrows():
            severity = str(row.get("severity", "LOW"))
            color_map = {"HIGH": "danger", "MEDIUM": "warning", "LOW": "info"}
            card = dbc.Card([
                dbc.CardBody([
                    dbc.Badge(severity, color=color_map.get(severity, "secondary"),
                              className="mb-1"),
                    html.H6(f"{row.get('ticker')} — {row.get('flag_type','').replace('_',' ')}",
                            className="mb-1", style={"fontSize": "13px"}),
                    html.P(str(row.get("description", ""))[:160] + "…",
                           style={"fontSize": "11px", "color": "#aaa", "marginBottom": 0}),
                    html.Small(str(row.get("flagged_at", ""))[:16],
                               style={"color": "#666"}),
                ])
            ], style={"width": "340px", "backgroundColor": "#16213e",
                      "border": "1px solid #2a2a4a"})
            cards.append(card)

        return df_to_records(flags_df), cards

    # ── Reset filters ────────────────────────────────────────────────────
    @app.callback(
        Output("filter-tickers",        "value"),
        Output("filter-forms",          "value"),
        Output("filter-roles",          "value"),
        Output("filter-insider-search", "value"),
        Output("filter-window",         "value"),
        Output("filter-min-return",     "value"),
        Output("filter-top-pct",        "value"),
        Input("btn-reset-filters",      "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_filters(_):
        return (
            [t for t, _ in COMPANIES],
            ["3", "4", "5"],
            ["director", "officer", "ten_pct"],
            "",
            "3m",
            0,
            100,
        )

"""
dashboard/components/tables.py

Dash AG Grid column definitions and helper functions.
"""

from __future__ import annotations
import pandas as pd


# ---------------------------------------------------------------------------
# Common AG Grid style
# ---------------------------------------------------------------------------

GRID_STYLE = {
    "height": "500px",
    "width": "100%",
}

_DEFAULT_COL = {
    "filter": True,
    "sortable": True,
    "resizable": True,
    "minWidth": 80,
    "floatingFilter": True,
}

_MONEY_FORMAT = {
    "function": "params.value != null ? '$' + params.value.toLocaleString('en-US', {maximumFractionDigits:0}) : ''"
}
_PCT_FORMAT = {
    "function": "params.value != null ? params.value.toFixed(1) + '%' : ''"
}
_SHARES_FORMAT = {
    "function": "params.value != null ? params.value.toLocaleString('en-US', {maximumFractionDigits:0}) : ''"
}


def _cell_style_pct(field: str) -> dict:
    """Green if positive, red if negative for percentage columns."""
    return {
        "function": (
            f"params.data && params.data['{field}'] != null "
            f"? (params.data['{field}'] >= 0 "
            f"? {{'color':'#00d4aa','fontWeight':'bold'}} "
            f": {{'color':'#ff4d6d','fontWeight':'bold'}}) "
            f": {{}}"
        )
    }


# ---------------------------------------------------------------------------
# Activity Feed table  (raw section16_filings)
# ---------------------------------------------------------------------------

ACTIVITY_COLUMNS = [
    {"field": "filing_date",      "headerName": "Filed",         "width": 110,
     "sort": "desc"},
    {"field": "ticker",           "headerName": "Ticker",        "width": 80},
    {"field": "insider_name",     "headerName": "Insider",       "width": 200},
    {"field": "officer_title",    "headerName": "Title",         "width": 180},
    {"field": "filing_form",      "headerName": "Form",          "width": 80},
    {"field": "transaction_date", "headerName": "Txn Date",      "width": 110},
    {"field": "transaction_code", "headerName": "Code",          "width": 70},
    {"field": "transaction_type", "headerName": "Type",          "width": 160},
    {"field": "shares",           "headerName": "Shares",        "width": 110,
     "valueFormatter": _SHARES_FORMAT},
    {"field": "price",            "headerName": "Price",         "width": 90,
     "valueFormatter": {"function": "params.value != null ? '$' + params.value.toFixed(2) : ''"}},
    {"field": "value",            "headerName": "Value",         "width": 120,
     "valueFormatter": _MONEY_FORMAT},
    {"field": "shares_remaining", "headerName": "Remaining",     "width": 120,
     "valueFormatter": _SHARES_FORMAT},
    {"field": "is_director",      "headerName": "Dir?",          "width": 60},
    {"field": "is_officer",       "headerName": "Off?",          "width": 60},
    {"field": "is_ten_pct_owner", "headerName": "10%+?",         "width": 65},
    {"field": "is_10b5_1_plan",   "headerName": "10b5-1?",       "width": 75},
]

ACTIVITY_ROW_STYLE = {
    "function": (
        "params.data && params.data.transaction_code === 'P' "
        "? {'backgroundColor': 'rgba(0,212,170,0.07)'} "
        ": params.data && params.data.transaction_code === 'S' "
        "? {'backgroundColor': 'rgba(255,77,109,0.07)'} "
        ": {}"
    )
}


# ---------------------------------------------------------------------------
# Leaderboard table  (insider_analytics)
# ---------------------------------------------------------------------------

LEADERBOARD_COLUMNS = [
    {"field": "ticker",                  "headerName": "Ticker",      "width": 80},
    {"field": "insider_name",            "headerName": "Insider",     "width": 200},
    {"field": "officer_title",           "headerName": "Title",       "width": 200},
    {"field": "is_director",             "headerName": "Dir",         "width": 55},
    {"field": "is_officer",              "headerName": "Off",         "width": 55},
    {"field": "is_ten_pct_owner",        "headerName": "10%+",        "width": 60},
    {"field": "first_txn_date",          "headerName": "First Txn",   "width": 110},
    {"field": "entry_price",             "headerName": "Entry $",     "width": 90,
     "valueFormatter": {"function": "params.value != null ? '$' + params.value.toFixed(2) : ''"}},
    {"field": "current_price",           "headerName": "Now $",       "width": 90,
     "valueFormatter": {"function": "params.value != null ? '$' + params.value.toFixed(2) : ''"}},
    {"field": "stock_pct_since_entry",   "headerName": "Since Entry", "width": 105,
     "valueFormatter": _PCT_FORMAT,
     "cellStyle": _cell_style_pct("stock_pct_since_entry")},
    {"field": "pct_2w",  "headerName": "2W %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_2w")},
    {"field": "pct_1m",  "headerName": "1M %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_1m")},
    {"field": "pct_3m",  "headerName": "3M %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_3m")},
    {"field": "pct_6m",  "headerName": "6M %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_6m")},
    {"field": "pct_1y",  "headerName": "1Y %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_1y")},
    {"field": "pct_2y",  "headerName": "2Y %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_2y")},
    {"field": "pct_3y",  "headerName": "3Y %",   "width": 80,
     "valueFormatter": _PCT_FORMAT, "cellStyle": _cell_style_pct("pct_3y")},
    {"field": "last_reported_shares",    "headerName": "Shares Held", "width": 120,
     "valueFormatter": _SHARES_FORMAT},
    {"field": "current_position_value",  "headerName": "Position $",  "width": 120,
     "valueFormatter": _MONEY_FORMAT},
    {"field": "open_mkt_unrealized_pct", "headerName": "Unrlzd P&L %","width": 110,
     "valueFormatter": _PCT_FORMAT,
     "cellStyle": _cell_style_pct("open_mkt_unrealized_pct")},
    {"field": "open_mkt_total_proceeds", "headerName": "Sale Proceeds","width": 130,
     "valueFormatter": _MONEY_FORMAT},
    {"field": "net_open_mkt_shares",     "headerName": "Net Shrs",    "width": 100,
     "valueFormatter": _SHARES_FORMAT},
    {"field": "n_open_mkt_buys",         "headerName": "# Buys",      "width": 80},
    {"field": "n_open_mkt_sells",        "headerName": "# Sells",     "width": 80},
]


# ---------------------------------------------------------------------------
# Flags table
# ---------------------------------------------------------------------------

FLAGS_COLUMNS = [
    {"field": "flagged_at",   "headerName": "Flagged",     "width": 140, "sort": "desc"},
    {"field": "severity",     "headerName": "Severity",    "width": 90,
     "cellStyle": {
         "function": (
             "params.value === 'HIGH' ? {'color':'#ff4d6d','fontWeight':'bold'} "
             ": params.value === 'MEDIUM' ? {'color':'#ffd700','fontWeight':'bold'} "
             ": {'color':'#a29bfe'}"
         )
     }},
    {"field": "ticker",       "headerName": "Ticker",      "width": 80},
    {"field": "flag_type",    "headerName": "Type",        "width": 160},
    {"field": "insider_name", "headerName": "Insider",     "width": 200},
    {"field": "description",  "headerName": "Description", "flex": 1, "wrapText": True,
     "autoHeight": True},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to AG Grid rowData, coercing dates to strings."""
    if df.empty:
        return []
    out = df.copy()
    for col in out.select_dtypes(include=["datetime64[ns]", "object"]).columns:
        try:
            out[col] = pd.to_datetime(out[col], errors="ignore")
            if hasattr(out[col], "dt"):
                out[col] = out[col].dt.strftime("%Y-%m-%d").where(out[col].notna(), None)
        except Exception:
            pass
    # Coerce date objects to strings
    import datetime
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].apply(
                lambda x: x.isoformat() if isinstance(x, (datetime.date, datetime.datetime)) else x
            )
    return out.where(pd.notna(out), None).to_dict("records")

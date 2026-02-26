"""
dashboard/app.py

Dash application entry point.
Gunicorn target: dashboard.app:server
"""

import dash
import dash_bootstrap_components as dbc

from dashboard.callbacks import register_callbacks
from dashboard.layout import build_layout

# ---------------------------------------------------------------------------
# Custom CSS injected inline (no external files needed)
# ---------------------------------------------------------------------------
_EXTRA_CSS = """
/* Sidebar labels */
.sidebar-label {
    color: #888;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
    display: block;
}

/* Tabs */
.custom-tabs { border-bottom: 1px solid #2a2a4a !important; }
.custom-tab {
    background-color: #16213e !important;
    color: #888 !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    padding: 10px 20px !important;
    font-size: 13px !important;
    transition: color 0.2s;
}
.custom-tab:hover { color: #e0e0e0 !important; }
.custom-tab--selected,
.custom-tab-selected {
    background-color: #1a1a2e !important;
    color: #4c8eff !important;
    border-bottom: 2px solid #4c8eff !important;
    font-weight: 600 !important;
}

/* Dropdown dark */
.dark-dropdown .Select-control,
.dark-dropdown .Select-menu-outer,
.dark-dropdown .VirtualizedSelectFocusedOption {
    background-color: #16213e !important;
    color: #e0e0e0 !important;
    border-color: #2a2a4a !important;
}
.Select-value-label { color: #e0e0e0 !important; }

/* AG Grid dark theme overrides */
.ag-theme-alpine-dark {
    --ag-background-color: #1a1a2e;
    --ag-header-background-color: #16213e;
    --ag-odd-row-background-color: #1e1e35;
    --ag-border-color: #2a2a4a;
    --ag-header-column-separator-color: #2a2a4a;
    --ag-font-size: 12px;
    --ag-row-hover-color: rgba(76,142,255,0.08);
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #1a1a2e; }
::-webkit-scrollbar-thumb { background: #2a2a4a; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #4c8eff; }

/* Bootstrap checklist labels */
.form-check-label { font-size: 13px; color: #ccc; }
.form-check-input:checked { background-color: #4c8eff; border-color: #4c8eff; }

/* Slider */
.rc-slider-track { background-color: #4c8eff; }
.rc-slider-handle { border-color: #4c8eff; }
"""

# ---------------------------------------------------------------------------
# Ensure DB tables exist on startup (safe to call repeatedly)
# ---------------------------------------------------------------------------
from db.session import init_db
init_db()

# ---------------------------------------------------------------------------
# App initialisation
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY,
                           dbc.icons.BOOTSTRAP],
    title="CeoWatcher",
    update_title=None,
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport",
                "content": "width=device-width, initial-scale=1"}],
)

app.index_string = app.index_string.replace(
    "</head>",
    f"<style>{_EXTRA_CSS}</style></head>",
)

app.layout = build_layout()
register_callbacks(app)

# Gunicorn needs this
server = app.server


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)

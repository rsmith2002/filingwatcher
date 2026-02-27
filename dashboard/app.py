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
# Custom CSS  —  "Carbon Amber" theme
# Fonts: Syne (UI) · JetBrains Mono (data)
# ---------------------------------------------------------------------------
_EXTRA_CSS = """
:root {
  --c-bg:        #080810;
  --c-surface:   #0d0e1b;
  --c-elevated:  #111225;
  --c-border:    #1c1e30;
  --c-border-hi: #272a40;
  --c-amber:     #f0a31a;
  --c-amber-lo:  rgba(240,163,26,0.10);
  --c-amber-hi:  #ffc04d;
  --c-green:     #17d890;
  --c-red:       #ff3d5a;
  --c-blue:      #5b8fff;
  --c-text:      #c6bead;
  --c-text-dim:  #5a5347;
  --c-text-hi:   #e8e0d2;
  --font-ui:    'Syne', system-ui, sans-serif;
  --font-mono:  'JetBrains Mono', ui-monospace, monospace;
}

body, html {
  background-color: var(--c-bg) !important;
  color: var(--c-text) !important;
}

/* ── Sidebar labels ── */
.sidebar-label {
  color: var(--c-amber) !important;
  font-family: var(--font-mono) !important;
  font-size: 9px !important;
  font-weight: 500 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.16em !important;
  margin-bottom: 8px !important;
  display: block !important;
  opacity: 0.85;
}

/* ── Tabs ── */
.custom-tabs {
  border-bottom: 1px solid var(--c-border) !important;
  background-color: var(--c-surface) !important;
}
.custom-tab {
  background-color: transparent !important;
  color: var(--c-text-dim) !important;
  border: none !important;
  border-bottom: 2px solid transparent !important;
  padding: 14px 28px !important;
  font-family: var(--font-ui) !important;
  font-size: 11px !important;
  font-weight: 700 !important;
  letter-spacing: 0.08em !important;
  text-transform: uppercase !important;
  transition: color 0.15s ease !important;
}
.custom-tab:hover {
  color: var(--c-text) !important;
  background-color: rgba(255,255,255,0.02) !important;
}
.custom-tab--selected,
.custom-tab-selected {
  background-color: transparent !important;
  color: var(--c-amber) !important;
  border-bottom: 2px solid var(--c-amber) !important;
  font-weight: 700 !important;
}

/* ── Dropdowns (Dash 1.x class names) ── */
.dark-dropdown .Select-control {
  background-color: var(--c-surface) !important;
  color: var(--c-text) !important;
  border: 1px solid var(--c-border) !important;
  font-family: var(--font-mono) !important;
  font-size: 12px !important;
}
.dark-dropdown .Select-menu-outer {
  background-color: var(--c-elevated) !important;
  border: 1px solid var(--c-border-hi) !important;
}
.dark-dropdown .VirtualizedSelectFocusedOption,
.dark-dropdown .Select-option.is-focused {
  background-color: var(--c-amber-lo) !important;
  color: var(--c-amber) !important;
}
.Select-value-label { color: var(--c-text) !important; }

/* ── AG Grid ── */
.ag-theme-alpine-dark {
  --ag-background-color:              var(--c-bg);
  --ag-header-background-color:       var(--c-surface);
  --ag-odd-row-background-color:      #0a0b14;
  --ag-border-color:                  var(--c-border);
  --ag-header-column-separator-color: var(--c-border);
  --ag-font-size:                     12px;
  --ag-row-hover-color:               var(--c-amber-lo);
  --ag-selected-row-background-color: var(--c-amber-lo);
  --ag-font-family:                   var(--font-mono);
  --ag-foreground-color:              var(--c-text);
  --ag-header-foreground-color:       var(--c-amber);
  --ag-cell-horizontal-padding:       14px;
}
.ag-theme-alpine-dark .ag-header-cell-label {
  font-size: 10px;
  letter-spacing: 0.07em;
  text-transform: uppercase;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--c-border-hi); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: var(--c-amber); }

/* ── Form controls ── */
.form-check-label {
  font-size: 12px !important;
  color: var(--c-text) !important;
  font-family: var(--font-ui) !important;
}
.form-check-input:checked {
  background-color: var(--c-amber) !important;
  border-color: var(--c-amber) !important;
}
.form-switch .form-check-input {
  background-color: var(--c-border-hi) !important;
  border-color: var(--c-border-hi) !important;
}
.form-switch .form-check-input:checked {
  background-color: var(--c-amber) !important;
  border-color: var(--c-amber) !important;
}
.form-control {
  background-color: var(--c-surface) !important;
  color: var(--c-text) !important;
  border: 1px solid var(--c-border) !important;
  font-family: var(--font-mono) !important;
  font-size: 12px !important;
  border-radius: 3px !important;
}
.form-control:focus {
  background-color: var(--c-surface) !important;
  color: var(--c-text) !important;
  border-color: var(--c-amber) !important;
  box-shadow: 0 0 0 2px var(--c-amber-lo) !important;
}
.input-group-text {
  background-color: var(--c-elevated) !important;
  color: var(--c-text-dim) !important;
  border: 1px solid var(--c-border) !important;
  font-family: var(--font-mono) !important;
  font-size: 12px !important;
}

/* ── Slider ── */
.rc-slider-track { background-color: var(--c-amber) !important; }
.rc-slider-rail  { background-color: var(--c-border) !important; }
.rc-slider-handle {
  border-color: var(--c-amber) !important;
  background-color: var(--c-surface) !important;
  box-shadow: 0 0 0 2px var(--c-amber-lo) !important;
  opacity: 1 !important;
}
.rc-slider-handle:hover,
.rc-slider-handle:active {
  border-color: var(--c-amber-hi) !important;
}
.rc-slider-mark-text {
  color: var(--c-text-dim) !important;
  font-size: 10px !important;
  font-family: var(--font-mono) !important;
}
.rc-slider-dot { border-color: var(--c-border) !important; }
.rc-slider-dot-active { border-color: var(--c-amber) !important; }

/* ── Reset button ── */
.btn-outline-secondary {
  color: var(--c-text-dim) !important;
  border-color: var(--c-border) !important;
  background: transparent !important;
  font-family: var(--font-mono) !important;
  font-size: 11px !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
}
.btn-outline-secondary:hover {
  background-color: var(--c-border) !important;
  color: var(--c-text) !important;
  border-color: var(--c-border-hi) !important;
}

/* ── HR divider ── */
hr { border-color: var(--c-border) !important; opacity: 1 !important; }
"""

# ---------------------------------------------------------------------------
# App initialisation
# ---------------------------------------------------------------------------

_GOOGLE_FONTS = (
    "https://fonts.googleapis.com/css2?"
    "family=Syne:wght@400;500;600;700;800&"
    "family=JetBrains+Mono:ital,wght@0,300;0,400;0,500;0,600;1,400&"
    "display=swap"
)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP, _GOOGLE_FONTS],
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


# ---------------------------------------------------------------------------
# /health — diagnostic endpoint, hit in browser to check DB connectivity
# ---------------------------------------------------------------------------
import json
from flask import jsonify

@server.route("/health")
def health():
    try:
        from db.session import get_session
        from db.models import Company, Section16Filing, InsiderAnalytics, PriceHistory, Flag
        import os

        session = get_session()
        result = {
            "status": "ok",
            "database_url_set": bool(os.environ.get("DATABASE_URL")),
            "database_url_preview": (os.environ.get("DATABASE_URL", "NOT SET")[:40] + "..."),
            "counts": {
                "companies":          session.query(Company).count(),
                "section16_filings":  session.query(Section16Filing).count(),
                "insider_analytics":  session.query(InsiderAnalytics).count(),
                "price_history":      session.query(PriceHistory).count(),
                "flags":              session.query(Flag).count(),
            },
            "sample_tickers": [
                r.ticker for r in session.query(Company).limit(5).all()
            ],
            "sample_filings": [
                {
                    "ticker": r.ticker,
                    "insider": r.insider_name,
                    "form": r.filing_form,
                    "code": r.transaction_code,
                    "is_director": r.is_director,
                    "is_officer": r.is_officer,
                }
                for r in session.query(Section16Filing).limit(5).all()
            ],
        }
        session.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)

"""
scripts/reset_filings.py

Clears section16_filings, insider_analytics, and flags so the backfill
can re-populate them cleanly.  Price history is LEFT intact.

Run once before re-running the backfill:
    python scripts/reset_filings.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.models import Flag, InsiderAnalytics, Section16Filing
from db.session import get_session, init_db

init_db()
session = get_session()
try:
    n_flags     = session.query(Flag).delete()
    n_analytics = session.query(InsiderAnalytics).delete()
    n_filings   = session.query(Section16Filing).delete()
    session.commit()
    print(f"Reset complete:")
    print(f"  section16_filings deleted : {n_filings}")
    print(f"  insider_analytics deleted : {n_analytics}")
    print(f"  flags deleted             : {n_flags}")
    print("Price history untouched.")
finally:
    session.close()

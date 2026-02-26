"""
ingestion/pipeline.py

Main entry point for the scheduled ingest job.
Run by Render's Cron Job service 3x daily.

Also importable for programmatic use:
    from ingestion.pipeline import run_pipeline
    run_pipeline()
"""

import sys
import traceback
from datetime import date, datetime, timedelta

from config import COMPANIES
from db.models import IngestRun, Section16Filing
from db.session import get_session, init_db
from ingestion.analytics import refresh_all_analytics
from ingestion.fetchers import upsert_large_holder_stakes, upsert_section16
from ingestion.flags import detect_and_save_flags
from ingestion.prices import sync_prices


def _last_successful_run_date(session) -> str:
    """Return the start date for the next ingest window."""
    last = (
        session.query(IngestRun)
        .filter_by(status="success")
        .order_by(IngestRun.run_at.desc())
        .first()
    )
    if last:
        # Re-fetch from 2 days before last run to catch amendments
        return str((last.run_at.date() - timedelta(days=2)))
    # First run — go back 30 days for the incremental pipeline
    # (backfill.py handles the full 10-year history)
    return str(date.today() - timedelta(days=30))


def run_pipeline(
    companies: list | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    verbose: bool = True,
) -> IngestRun:
    """
    Execute the full ingest pipeline:
      1. Ensure DB tables exist
      2. Sync prices for all tickers
      3. Fetch new Section 16 filings and upsert
      4. Fetch new large-holder stakes and upsert
      5. Detect and save flags for newly inserted filings
      6. Refresh insider analytics
      7. Log the run

    Returns the IngestRun record.
    """
    init_db()
    session = get_session()
    run = IngestRun(run_at=datetime.utcnow(), status="running")
    session.add(run)
    session.commit()

    if companies is None:
        companies = COMPANIES
    if end_date is None:
        end_date = str(date.today())
    if start_date is None:
        start_date = _last_successful_run_date(session)

    print(f"\n{'='*60}")
    print(f"CeoWatcher pipeline  {datetime.utcnow():%Y-%m-%d %H:%M} UTC")
    print(f"Window: {start_date} → {end_date}")
    print(f"{'='*60}")

    errors = []
    new_s16 = new_stakes = analytics_count = flags_count = 0
    tickers = [t for t, _ in companies]

    # ── 1. Prices ─────────────────────────────────────────────────────────
    try:
        print("\n[1/5] Syncing prices …")
        sync_prices(tickers=tickers, verbose=verbose)
    except Exception as exc:
        errors.append(f"prices: {exc}")
        print(f"  ERROR in price sync: {exc}")

    # ── 2. Section 16 filings ─────────────────────────────────────────────
    try:
        print("\n[2/5] Fetching Section 16 filings …")
        new_s16 = upsert_section16(
            companies=companies,
            start_date=start_date,
            end_date=end_date,
            verbose=verbose,
        )
        print(f"  → {new_s16} new rows inserted")
    except Exception as exc:
        errors.append(f"section16: {exc}")
        print(f"  ERROR in section16 fetch: {exc}")
        traceback.print_exc()

    # ── 3. Large holder stakes ────────────────────────────────────────────
    try:
        print("\n[3/5] Fetching large-holder stakes (13D/13G) …")
        new_stakes = upsert_large_holder_stakes(
            companies=companies,
            start_date=start_date,
            end_date=end_date,
            verbose=verbose,
        )
        print(f"  → {new_stakes} new rows inserted")
    except Exception as exc:
        errors.append(f"stakes: {exc}")
        print(f"  ERROR in stakes fetch: {exc}")

    # ── 4. Flags ──────────────────────────────────────────────────────────
    try:
        print("\n[4/5] Detecting flags …")
        # Get IDs of section16 rows inserted in this run
        # (rows created_at >= run start)
        new_ids = [
            r.id for r in session.query(Section16Filing.id)
            .filter(Section16Filing.created_at >= run.run_at)
            .all()
        ]
        if new_ids:
            flags_count = detect_and_save_flags(new_ids, verbose=verbose)
        else:
            print("  No new filings to flag")
    except Exception as exc:
        errors.append(f"flags: {exc}")
        print(f"  ERROR in flag detection: {exc}")

    # ── 5. Analytics ──────────────────────────────────────────────────────
    try:
        print("\n[5/5] Refreshing analytics …")
        analytics_count = refresh_all_analytics(verbose=verbose)
        print(f"  → {analytics_count} insider records updated")
    except Exception as exc:
        errors.append(f"analytics: {exc}")
        print(f"  ERROR in analytics refresh: {exc}")

    # ── Finalise run log ──────────────────────────────────────────────────
    run.companies_processed  = len(companies)
    run.new_section16_rows   = new_s16
    run.new_stakes_rows      = new_stakes
    run.analytics_refreshed  = analytics_count
    run.flags_raised         = flags_count
    run.status               = "partial" if errors else "success"
    run.errors               = "\n".join(errors) if errors else None
    session.commit()
    session.close()

    print(f"\n{'='*60}")
    print(f"Pipeline complete — status: {run.status}")
    print(f"  Section 16 new rows : {new_s16}")
    print(f"  Stakes new rows     : {new_stakes}")
    print(f"  Analytics updated   : {analytics_count}")
    print(f"  Flags raised        : {flags_count}")
    if errors:
        print(f"  ERRORS: {'; '.join(errors)}")
    print(f"{'='*60}\n")

    return run


if __name__ == "__main__":
    run = run_pipeline()
    sys.exit(0 if run.status in ("success", "partial") else 1)

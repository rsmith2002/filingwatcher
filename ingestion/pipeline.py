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
from pathlib import Path

# Ensure project root is on sys.path when run as a script or from GitHub Actions
sys.path.insert(0, str(Path(__file__).parent.parent))

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

    if companies is None:
        companies = COMPANIES
    if end_date is None:
        end_date = str(date.today())

    # Short-lived session: create the run record + determine start date,
    # then close immediately.  Neon drops serverless SSL connections after
    # several minutes of inactivity so we never hold a session across the
    # full pipeline run.
    _init = get_session()
    try:
        run = IngestRun(run_at=datetime.utcnow(), status="running")
        _init.add(run)
        _init.commit()
        run_id    = run.id
        run_start = run.run_at
        if start_date is None:
            start_date = _last_successful_run_date(_init)
    finally:
        _init.close()

    print(f"\n{'='*60}")
    print(f"CeoWatcher pipeline  {datetime.utcnow():%Y-%m-%d %H:%M} UTC")
    print(f"Window: {start_date} → {end_date}")
    print(f"{'='*60}")

    # Seed companies table (idempotent — skips existing rows)
    from db.models import Company
    from ingestion.fetchers import _resolve_company
    _seed = get_session()
    try:
        for ticker, display_name in companies:
            if not _seed.get(Company, ticker):
                ec = _resolve_company(ticker, verbose=False)
                _seed.merge(Company(
                    ticker=ticker,
                    name=ec.name if ec else display_name,
                    cik=str(ec.cik) if ec else None,
                ))
        _seed.commit()
    finally:
        _seed.close()

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
        _flag_s = get_session()
        try:
            new_ids = [
                r.id for r in _flag_s.query(Section16Filing.id)
                .filter(Section16Filing.created_at >= run_start)
                .all()
            ]
        finally:
            _flag_s.close()
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

    # ── Finalise run log (fresh session — original may have timed out) ────
    _log = get_session()
    try:
        run_rec = _log.get(IngestRun, run_id)
        run_rec.companies_processed  = len(companies)
        run_rec.new_section16_rows   = new_s16
        run_rec.new_stakes_rows      = new_stakes
        run_rec.analytics_refreshed  = analytics_count
        run_rec.flags_raised         = flags_count
        run_rec.status               = "partial" if errors else "success"
        run_rec.errors               = "\n".join(errors) if errors else None
        _log.commit()
    except Exception as exc:
        _log.rollback()
        print(f"  WARN: could not update run log: {exc}")
    finally:
        _log.close()

    final_status = "partial" if errors else "success"
    print(f"\n{'='*60}")
    print(f"Pipeline complete — status: {final_status}")
    print(f"  Section 16 new rows : {new_s16}")
    print(f"  Stakes new rows     : {new_stakes}")
    print(f"  Analytics updated   : {analytics_count}")
    print(f"  Flags raised        : {flags_count}")
    if errors:
        print(f"  ERRORS: {'; '.join(errors)}")
    print(f"{'='*60}\n")

    return run_rec if "run_rec" in dir() else None


if __name__ == "__main__":
    result = run_pipeline()
    status = getattr(result, "status", "partial") if result else "partial"
    sys.exit(0 if status in ("success", "partial") else 1)

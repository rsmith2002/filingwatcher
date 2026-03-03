"""
scripts/backfill_flags.py

Retroactively run flag detection over ALL historical Section16Filing rows.

The main backfill.py script imports/inserts filings but never ran
detect_and_save_flags(), so the flags table is nearly empty.
This script fills that gap so the Backtesting tab has a full signal history.

Run once (or re-run safely — _already_flagged() deduplicates):
    python scripts/backfill_flags.py

Options:
    --batch-size N   Filings processed per batch (default: 500)
    --quiet          Suppress per-batch output
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.models import Section16Filing
from db.session import get_session, init_db
from ingestion.flags import detect_and_save_flags


def run_backfill_flags(batch_size: int = 500, verbose: bool = True) -> int:
    """
    Query ALL Section16Filing IDs ordered chronologically and call
    detect_and_save_flags() in batches.

    Processing oldest-first is important: the cluster_buy, reversal, and
    conviction detectors look at prior history, so chronological order
    ensures those lookups see the same data they would have at ingest time.

    Returns total count of flags created.
    """
    init_db()

    # ── Count total filings ───────────────────────────────────────────────
    session = get_session()
    try:
        total = session.query(Section16Filing.id).count()
    finally:
        session.close()

    if total == 0:
        print("No Section16Filing rows found — run backfill.py first.")
        return 0

    print(f"\n{'='*60}")
    print(f"CeoWatcher FLAG BACKFILL")
    print(f"  Total filings  : {total:,}")
    print(f"  Batch size     : {batch_size}")
    print(f"  Total batches  : {(total + batch_size - 1) // batch_size}")
    print(f"{'='*60}\n")

    total_flags = 0
    offset = 0
    batch_num = 0

    while offset < total:
        # Fetch IDs in chronological order
        session = get_session()
        try:
            ids = [
                row[0]
                for row in (
                    session.query(Section16Filing.id)
                    .order_by(Section16Filing.transaction_date, Section16Filing.id)
                    .offset(offset)
                    .limit(batch_size)
                    .all()
                )
            ]
        finally:
            session.close()

        if not ids:
            break

        batch_num += 1
        if verbose:
            pct = min(offset + len(ids), total) / total * 100
            print(f"  Batch {batch_num:4d}: rows {offset+1:>7,}–{offset+len(ids):>7,} "
                  f"({pct:.1f}%)  ...", end="", flush=True)

        try:
            n = detect_and_save_flags(ids, verbose=False)
            total_flags += n
            if verbose:
                print(f"  {n} new flags  (running total: {total_flags:,})")
        except Exception as exc:
            if verbose:
                print(f"  ERROR: {exc}")

        offset += batch_size

    print(f"\n{'='*60}")
    print(f"Flag backfill complete.  Total flags created: {total_flags:,}")
    print(f"{'='*60}\n")
    return total_flags


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical flag detection")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Filings per batch (default: 500)")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    run_backfill_flags(batch_size=args.batch_size, verbose=not args.quiet)

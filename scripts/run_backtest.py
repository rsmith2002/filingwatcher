"""
scripts/run_backtest.py

Run the CeoWatcher backtesting engine locally and save results to the
`backtest_cache` table in the Neon DB.  The dashboard then reads from
this cache instead of running the heavy simulation in-process on Render.

Usage
-----
    python scripts/run_backtest.py
    python scripts/run_backtest.py --capital 50000 --base-pct 0.03
    python scripts/run_backtest.py --max-hold 60 --stop-loss 0.08

All params are optional; defaults match the dashboard UI values.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime

# ── path fix so imports resolve from the repo root ────────────────────────────
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from db.session import get_session, init_db
from db.models import BacktestCache
from ingestion.backtest import run_backtest


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _default_serial(obj):
    """JSON encoder for types that aren't serialisable by default."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if hasattr(obj, "item"):          # numpy scalar
        return obj.item()
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")


def _serialise_result(result) -> str:
    """Convert a BacktestResult to a JSON string safe for DB storage."""
    import pandas as pd

    def _series_to_dict(s: pd.Series) -> dict:
        return {str(k): (None if v != v else float(v)) for k, v in s.items()}

    def _df_to_records(df: pd.DataFrame) -> list[dict]:
        if df.empty:
            return []
        out = df.copy()
        for col in out.columns:
            if out[col].dtype == "object":
                out[col] = out[col].apply(
                    lambda x: x.isoformat() if isinstance(x, (date, datetime)) else x
                )
            elif hasattr(out[col].iloc[0] if len(out) else None, "isoformat"):
                out[col] = out[col].apply(
                    lambda x: x.isoformat() if x is not None else None
                )
        # Convert date columns
        for col in out.select_dtypes(include=["datetime64[ns]"]).columns:
            out[col] = out[col].dt.strftime("%Y-%m-%d")
        return out.where(out.notna(), None).to_dict("records")

    payload = {
        "trades":  _df_to_records(result.trades_df),
        "equity":  _series_to_dict(result.equity_series),
        "spy":     _series_to_dict(result.spy_series),
        "stats":   result.stats,
        "params":  result.params,
    }
    return json.dumps(payload, default=_default_serial)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run CeoWatcher backtest and cache results.")
    parser.add_argument("--capital",    type=float, default=100_000,   help="Starting capital (default: 100000)")
    parser.add_argument("--base-pct",   type=float, default=0.05,      help="Base position pct (default: 0.05)")
    parser.add_argument("--max-hold",   type=int,   default=90,        help="Max holding days (default: 90)")
    parser.add_argument("--stop-loss",  type=float, default=0.10,      help="Stop-loss pct (default: 0.10)")
    parser.add_argument("--slippage",   type=float, default=0.001,     help="Slippage pct (default: 0.001)")
    parser.add_argument("--rfr",        type=float, default=0.05,      help="Risk-free rate (default: 0.05)")
    parser.add_argument("--start-date", type=str,   default="2021-01-01", help="Earliest flag filing date (default: 2021-01-01)")
    args = parser.parse_args()

    print("=" * 60)
    print("CeoWatcher Backtest Runner")
    print("=" * 60)
    print(f"  Capital:       ${args.capital:,.0f}")
    print(f"  Base pct:      {args.base_pct*100:.1f}%  (HIGH=3× MEDIUM=2× LOW=1×)")
    print(f"  Max hold:      {args.max_hold} days")
    print(f"  Stop loss:     {args.stop_loss*100:.1f}%")
    print(f"  Slippage:      {args.slippage*100:.2f}%")
    print(f"  Risk-free:     {args.rfr*100:.1f}%")
    print(f"  Start date:    {args.start_date}")
    print()

    # Ensure DB schema is up to date (creates backtest_cache if missing)
    print("Initialising database …")
    init_db()

    print("Loading data from database …")
    print("Running simulation …")

    session = get_session()
    try:
        from datetime import date as _date
        sd = _date.fromisoformat(args.start_date)

        result = run_backtest(
            starting_capital=args.capital,
            base_pct=args.base_pct,
            max_holding_days=args.max_hold,
            stop_loss_pct=args.stop_loss,
            slippage_pct=args.slippage,
            risk_free_rate=args.rfr,
            start_date=sd,
        )

        if result is None:
            print("\n[ERROR] Not enough data to run backtest (no flags or no prices).")
            sys.exit(1)

        print(f"\nSimulation complete.  {len(result.trades_df)} trades processed.")

        # Print key stats
        s = result.stats
        print()
        print("── Key Statistics ──────────────────────────────────────")
        print(f"  Total return:    {s.get('total_return_pct', 'N/A'):>8}%")
        print(f"  CAGR:            {s.get('cagr_pct', 'N/A'):>8}%")
        print(f"  Sharpe:          {s.get('sharpe', 'N/A'):>8}")
        print(f"  Sortino:         {s.get('sortino', 'N/A'):>8}")
        print(f"  Max drawdown:    {s.get('max_drawdown_pct', 'N/A'):>8}%")
        print(f"  Calmar:          {s.get('calmar', 'N/A'):>8}")
        print(f"  Win rate:        {s.get('win_rate_pct', 'N/A'):>8}%")
        print(f"  Profit factor:   {s.get('profit_factor', 'N/A'):>8}")
        print(f"  Total trades:    {s.get('total_trades', 'N/A'):>8}")
        if s.get("alpha_pct") is not None:
            print(f"  Alpha (ann.):    {s['alpha_pct']:>8}%")
            print(f"  Beta:            {s['beta']:>8}")
            print(f"  SPY total ret:   {s.get('spy_total_return_pct', 'N/A'):>8}%")
            print(f"  Excess return:   {s.get('excess_return_pct', 'N/A'):>8}%")
        print()

        # Serialise and save to DB
        print("Saving results to database …")
        params_json  = json.dumps(result.params, default=_default_serial)
        results_json = _serialise_result(result)

        # Delete old cache rows (keep only the latest run)
        session.query(BacktestCache).delete()

        cache = BacktestCache(
            computed_at=datetime.utcnow(),
            params_json=params_json,
            results_json=results_json,
            status="ok",
        )
        session.add(cache)
        session.commit()

        print("Results saved.  The dashboard will now show the cached backtest.")
        print("=" * 60)

    except Exception as exc:
        session.rollback()
        # Save error to cache so dashboard can show a message
        try:
            session.query(BacktestCache).delete()
            cache = BacktestCache(
                computed_at=datetime.utcnow(),
                params_json=json.dumps({}),
                results_json=json.dumps({}),
                status="error",
                error_msg=str(exc),
            )
            session.add(cache)
            session.commit()
        except Exception:
            pass
        print(f"\n[ERROR] Backtest failed: {exc}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()

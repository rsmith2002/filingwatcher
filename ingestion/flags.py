"""
ingestion/flags.py

Detects interesting filing patterns and writes Flag records to the database.
Called after each ingestion run with the newly inserted Section16Filing rows.
"""

from datetime import date, timedelta
from typing import Optional

from db.models import Flag, Section16Filing
from db.session import get_session


# ---------------------------------------------------------------------------
# Flag type constants
# ---------------------------------------------------------------------------
class FlagType:
    CEO_CFO_PURCHASE    = "CEO_CFO_PURCHASE"      # CEO or CFO buys on open market
    CLUSTER_BUY         = "CLUSTER_BUY"           # 3+ insiders buy same company within 7 days
    LARGE_PURCHASE      = "LARGE_PURCHASE"         # Single open-market buy > $500k
    FIRST_PURCHASE      = "FIRST_PURCHASE"         # Insider's first ever Code=P in our data
    ACTIVIST_13D        = "ACTIVIST_13D"           # New Schedule 13D filed
    THRESHOLD_CROSS     = "THRESHOLD_CROSS"        # 10%+ owner crosses 5/10/15/20% threshold
    REVERSAL_BUY        = "REVERSAL_BUY"           # (legacy) Insider who only sold is now buying
    BULL_REVERSAL       = "BULL_REVERSAL"          # 3+ consecutive sells OR 1yr selling → now buying
    CONVICTION_BUY      = "CONVICTION_BUY"         # Purchase increases holdings by ≥10%
    DIP_BUY             = "DIP_BUY"               # Buying when stock is down ≥50% (1yr) or ≥20% (1mo)


def _already_flagged(session, accession_no: str, flag_type: str) -> bool:
    return session.query(Flag).filter_by(
        accession_no=accession_no, flag_type=flag_type
    ).first() is not None


# ---------------------------------------------------------------------------
# Individual detectors
# ---------------------------------------------------------------------------

def _flag_ceo_cfo_purchases(new_filings: list, session) -> list[Flag]:
    flags = []
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        title = (f.officer_title or "").lower()
        if not any(k in title for k in ("chief executive", "ceo", "chief financial", "cfo")):
            continue
        if _already_flagged(session, f.accession_no, FlagType.CEO_CFO_PURCHASE):
            continue
        value_str = f"${f.value:,.0f}" if f.value else "unknown value"
        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.CEO_CFO_PURCHASE,
            severity="HIGH",
            description=(
                f"{f.insider_name} ({f.officer_title}) bought {f.shares:,.0f} shares "
                f"of {f.ticker} on {f.transaction_date} at ${f.price:.2f} "
                f"(total {value_str}). Open-market CEO/CFO purchases are rare and "
                f"highly informative signals."
            ),
        ))
    return flags


def _flag_large_purchases(new_filings: list, session) -> list[Flag]:
    flags = []
    threshold = 500_000
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if not f.value or f.value < threshold:
            continue
        if _already_flagged(session, f.accession_no, FlagType.LARGE_PURCHASE):
            continue
        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.LARGE_PURCHASE,
            severity="HIGH" if f.value >= 2_000_000 else "MEDIUM",
            description=(
                f"{f.insider_name} made an open-market purchase of "
                f"{f.shares:,.0f} shares of {f.ticker} worth "
                f"${f.value:,.0f} on {f.transaction_date}."
            ),
        ))
    return flags


def _flag_cluster_buys(new_filings: list, session) -> list[Flag]:
    """Flag when 3+ distinct insiders buy the same ticker within a 7-day window."""
    flags = []
    buys = [f for f in new_filings if f.transaction_code == "P" and not f.is_derivative
            and f.transaction_date is not None]

    # Group by ticker
    from collections import defaultdict
    by_ticker = defaultdict(list)
    for f in buys:
        by_ticker[f.ticker].append(f)

    for ticker, ticker_buys in by_ticker.items():
        ticker_buys.sort(key=lambda x: x.transaction_date)
        for i, anchor in enumerate(ticker_buys):
            window_end = anchor.transaction_date + timedelta(days=7)
            cluster = [b for b in ticker_buys[i:]
                       if b.transaction_date <= window_end]
            insiders_in_cluster = {b.insider_name for b in cluster}
            if len(insiders_in_cluster) >= 3:
                cluster_accno = min(b.accession_no for b in cluster)
                if _already_flagged(session, cluster_accno, FlagType.CLUSTER_BUY):
                    continue
                names = ", ".join(sorted(insiders_in_cluster)[:5])
                flags.append(Flag(
                    ticker=ticker,
                    insider_name=names,
                    accession_no=cluster_accno,
                    flag_type=FlagType.CLUSTER_BUY,
                    severity="HIGH",
                    description=(
                        f"{len(insiders_in_cluster)} insiders bought {ticker} "
                        f"within 7 days of each other "
                        f"(around {anchor.transaction_date}): {names}. "
                        f"Cluster buys are among the strongest insider signals."
                    ),
                ))
                break  # only flag once per ticker per run
    return flags


def _flag_first_purchases(new_filings: list, session) -> list[Flag]:
    """Flag an insider's first ever open-market purchase in our dataset."""
    flags = []
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if _already_flagged(session, f.accession_no, FlagType.FIRST_PURCHASE):
            continue
        # Check if this insider has any prior Code=P in the DB (excluding this filing)
        prior = session.query(Section16Filing).filter(
            Section16Filing.ticker == f.ticker,
            Section16Filing.insider_name == f.insider_name,
            Section16Filing.transaction_code == "P",
            Section16Filing.accession_no != f.accession_no,
        ).first()
        if prior:
            continue
        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.FIRST_PURCHASE,
            severity="MEDIUM",
            description=(
                f"{f.insider_name} ({f.officer_title or 'Insider'}) made their first "
                f"recorded open-market purchase of {f.ticker}: "
                f"{f.shares:,.0f} shares at ${f.price:.2f} on {f.transaction_date}."
            ),
        ))
    return flags


def _flag_reversal_buys(new_filings: list, session) -> list[Flag]:
    """(Legacy) Flag insiders who have exclusively sold in the past 90 days but are now buying."""
    flags = []
    cutoff = date.today() - timedelta(days=90)
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if _already_flagged(session, f.accession_no, FlagType.REVERSAL_BUY):
            continue
        # Recent activity for this insider
        recent = session.query(Section16Filing).filter(
            Section16Filing.ticker == f.ticker,
            Section16Filing.insider_name == f.insider_name,
            Section16Filing.transaction_date >= cutoff,
            Section16Filing.transaction_code.in_(["P", "S"]),
            Section16Filing.accession_no != f.accession_no,
        ).all()
        had_sells  = any(r.transaction_code == "S" for r in recent)
        had_buys   = any(r.transaction_code == "P" for r in recent)
        if had_sells and not had_buys:
            flags.append(Flag(
                ticker=f.ticker,
                insider_name=f.insider_name,
                accession_no=f.accession_no,
                flag_type=FlagType.REVERSAL_BUY,
                severity="MEDIUM",
                description=(
                    f"{f.insider_name} had been selling {f.ticker} in the last 90 days "
                    f"but just made an open-market PURCHASE of {f.shares:,.0f} shares "
                    f"at ${f.price:.2f} on {f.transaction_date} — a potential reversal signal."
                ),
            ))
    return flags


def _flag_bull_reversals(new_filings: list, session) -> list[Flag]:
    """
    Strong reversal signal: insider who had 3+ consecutive sells OR was selling
    for 1+ year has now flipped to buying.
    """
    flags = []
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if not f.transaction_date:
            continue
        if _already_flagged(session, f.accession_no, FlagType.BULL_REVERSAL):
            continue

        # Get all prior P/S transactions for this insider+ticker, most recent first
        prior = session.query(Section16Filing).filter(
            Section16Filing.ticker == f.ticker,
            Section16Filing.insider_name == f.insider_name,
            Section16Filing.transaction_code.in_(["P", "S"]),
            Section16Filing.is_derivative == False,
            Section16Filing.transaction_date < f.transaction_date,
        ).order_by(Section16Filing.transaction_date.desc()).all()

        if not prior:
            continue

        # Condition A: count consecutive sells from most recent backward
        consecutive_sells = 0
        for txn in prior:
            if txn.transaction_code == "S":
                consecutive_sells += 1
            else:
                break  # hit a non-sell — stop counting

        # Condition B: any sells spanning ≥ 365 days before this buy, with no prior P
        all_prior_sells = [t for t in prior if t.transaction_code == "S"]
        all_prior_buys  = [t for t in prior if t.transaction_code == "P"]
        sell_span_days  = 0
        if all_prior_sells and not all_prior_buys:
            oldest_sell = min(all_prior_sells, key=lambda t: t.transaction_date)
            sell_span_days = (f.transaction_date - oldest_sell.transaction_date).days

        if consecutive_sells >= 3:
            desc = (
                f"{f.insider_name} has been selling {f.ticker} for "
                f"{consecutive_sells} consecutive transactions but just made an "
                f"open-market BUY of {f.shares:,.0f} shares at ${f.price:.2f} "
                f"on {f.transaction_date}. Flipping after sustained selling is a "
                f"strong bullish reversal signal."
            )
        elif sell_span_days >= 365:
            years = sell_span_days / 365
            desc = (
                f"{f.insider_name} had been exclusively selling {f.ticker} for "
                f"{years:.1f} years but just made an open-market BUY of "
                f"{f.shares:,.0f} shares at ${f.price:.2f} on {f.transaction_date}. "
                f"Buying after over a year of selling is a strong reversal signal."
            )
        else:
            continue

        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.BULL_REVERSAL,
            severity="HIGH",
            description=desc,
        ))
    return flags


def _flag_conviction_buys(new_filings: list, session) -> list[Flag]:
    """
    Flag purchases that significantly increase the insider's reported holdings.
    shares_remaining is the post-transaction balance; prior = shares_remaining - shares.
    ≥25% increase → HIGH, ≥10% increase → MEDIUM.
    """
    flags = []
    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if not f.shares or not f.shares_remaining:
            continue
        prior_shares = f.shares_remaining - f.shares
        if prior_shares <= 0:
            continue
        pct_increase = f.shares / prior_shares
        if pct_increase < 0.10:
            continue
        if _already_flagged(session, f.accession_no, FlagType.CONVICTION_BUY):
            continue

        severity = "HIGH" if pct_increase >= 0.25 else "MEDIUM"
        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.CONVICTION_BUY,
            severity=severity,
            description=(
                f"{f.insider_name} increased their {f.ticker} position by "
                f"{pct_increase * 100:.0f}% — bought {f.shares:,.0f} shares "
                f"(prior balance: {prior_shares:,.0f}) on {f.transaction_date}. "
                f"Large % position increases are historically the strongest "
                f"insider buy signals."
            ),
        ))
    return flags


def _flag_dip_buys(new_filings: list, session) -> list[Flag]:
    """
    Flag insiders buying when the stock is significantly depressed:
    - Down ≥50% from 1 year ago (deep value buy)
    - Down ≥20% from 1 month ago (catching a falling knife or genuine dip)
    """
    from db.models import PriceHistory
    flags = []

    for f in new_filings:
        if f.transaction_code != "P" or f.is_derivative:
            continue
        if not f.transaction_date:
            continue
        if _already_flagged(session, f.accession_no, FlagType.DIP_BUY):
            continue

        # Use transaction price if available, else look up close from DB
        txn_price = f.price if (f.price and f.price > 0) else None
        if not txn_price:
            row = session.query(PriceHistory.close).filter(
                PriceHistory.ticker == f.ticker,
                PriceHistory.date >= f.transaction_date,
            ).order_by(PriceHistory.date).first()
            if row:
                txn_price = float(row[0])
        if not txn_price:
            continue

        date_1yr = f.transaction_date - timedelta(days=365)
        date_1mo = f.transaction_date - timedelta(days=30)

        row_1yr = session.query(PriceHistory.close).filter(
            PriceHistory.ticker == f.ticker,
            PriceHistory.date <= date_1yr,
        ).order_by(PriceHistory.date.desc()).first()

        row_1mo = session.query(PriceHistory.close).filter(
            PriceHistory.ticker == f.ticker,
            PriceHistory.date <= date_1mo,
        ).order_by(PriceHistory.date.desc()).first()

        reasons = []
        if row_1yr:
            pct_1yr = (txn_price / float(row_1yr[0]) - 1) * 100
            if pct_1yr <= -50:
                reasons.append(f"down {abs(pct_1yr):.0f}% vs. 1 year ago")
        if row_1mo:
            pct_1mo = (txn_price / float(row_1mo[0]) - 1) * 100
            if pct_1mo <= -20:
                reasons.append(f"down {abs(pct_1mo):.0f}% vs. 1 month ago")

        if not reasons:
            continue

        flags.append(Flag(
            ticker=f.ticker,
            insider_name=f.insider_name,
            accession_no=f.accession_no,
            flag_type=FlagType.DIP_BUY,
            severity="HIGH",
            description=(
                f"{f.insider_name} bought {f.shares:,.0f} shares of {f.ticker} "
                f"at ${txn_price:.2f} on {f.transaction_date} while the stock was "
                f"{' and '.join(reasons)}. Insiders buying into significant drawdowns "
                f"have the highest historical forward returns."
            ),
        ))
    return flags


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_and_save_flags(new_filing_ids: list[int], verbose: bool = True) -> int:
    """
    Run all flag detectors against the given list of newly inserted
    Section16Filing IDs and write Flag records to the database.

    Returns count of new flags raised.
    """
    session = get_session()
    total = 0
    try:
        new_filings = (
            session.query(Section16Filing)
            .filter(Section16Filing.id.in_(new_filing_ids))
            .all()
        )
        if not new_filings:
            return 0

        all_flags = (
            _flag_ceo_cfo_purchases(new_filings, session)
            + _flag_large_purchases(new_filings, session)
            + _flag_cluster_buys(new_filings, session)
            + _flag_first_purchases(new_filings, session)
            + _flag_reversal_buys(new_filings, session)
            + _flag_bull_reversals(new_filings, session)
            + _flag_conviction_buys(new_filings, session)
            + _flag_dip_buys(new_filings, session)
        )

        for flag in all_flags:
            session.add(flag)
            total += 1

        session.commit()
        if verbose and total:
            print(f"  {total} new flag(s) raised")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return total

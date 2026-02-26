"""
ingestion/fetchers.py

Wraps the edgartools filing fetch logic from sp500_insider_transactions.py
and writes rows directly into the PostgreSQL database instead of CSV files.

Returns counts of newly inserted rows.
"""

import math
import sys
from pathlib import Path
from typing import Optional

# Support both local (dev) vendored edgartools and installed package (Render)
_local = Path(__file__).parent.parent / "edgartools"
if _local.exists():
    sys.path.insert(0, str(_local))

from config import EDGAR_IDENTITY, COMPANIES
from db.models import Company, Section16Filing, LargeHolderStake
from db.session import get_session


# ---------------------------------------------------------------------------
# Helpers (shared with original script)
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _ownership_str(direct_indirect: str, nature: str) -> str:
    if direct_indirect == "D":
        return "Direct"
    if direct_indirect == "I":
        return f"Indirect ({nature})" if nature else "Indirect"
    return direct_indirect or ""


def _get_owners_meta(form_obj) -> dict:
    owners = getattr(form_obj, "reporting_owners", None)
    if not owners or len(owners) == 0:
        return dict(insider_name=None, insider_cik=None, is_director=None,
                    is_officer=None, is_ten_pct_owner=None,
                    officer_title=None, position=None)
    names     = " / ".join(o.name          for o in owners.owners if o.name)
    ciks      = " / ".join(str(o.cik)      for o in owners.owners if o.cik)
    titles    = " / ".join(o.officer_title for o in owners.owners if o.officer_title)
    positions = " / ".join(o.position      for o in owners.owners if o.position)
    return dict(
        insider_name     = names     or None,
        insider_cik      = ciks      or None,
        is_director      = any(o.is_director      for o in owners.owners),
        is_officer       = any(o.is_officer        for o in owners.owners),
        is_ten_pct_owner = any(o.is_ten_pct_owner  for o in owners.owners),
        officer_title    = titles    or None,
        position         = positions or None,
    )


def _resolve_company(ticker: str, verbose: bool):
    from edgar import Company as ECompany
    try:
        from edgar.entity import CompanyNotFoundError
    except ImportError:
        from edgar.exceptions import NoFilingsFound as CompanyNotFoundError

    try:
        return ECompany(ticker)
    except Exception:
        alt = {"BRK-B": "BRK.B", "BRK.B": "BRK-B"}.get(ticker)
        if alt:
            try:
                c = ECompany(alt)
                if verbose:
                    print(f"  (resolved via alternate ticker {alt})")
                return c
            except Exception:
                pass
        if verbose:
            print(f"  ERROR: Could not find company for ticker {ticker} — skipping.")
        return None


# ---------------------------------------------------------------------------
# Section 16 parsers
# ---------------------------------------------------------------------------

def _rows_from_form3(form3, ticker: str, company_name: str, filing) -> list[dict]:
    rows = []
    meta = _get_owners_meta(form3)
    base = dict(
        ticker=ticker, company_name=company_name,
        filing_date=filing.filing_date, accession_no=filing.accession_no,
        filing_form=filing.form, **meta
    )
    nd = getattr(form3, "non_derivative_table", None)
    if nd and not nd.holdings.empty:
        for _, row in nd.holdings.data.iterrows():
            rows.append({**base,
                "transaction_date": None, "security_title": row.get("Security"),
                "transaction_code": None, "transaction_type": "Initial Ownership",
                "acquired_disposed": "A", "shares": _safe_float(row.get("Shares")),
                "price": None, "value": None,
                "shares_remaining": _safe_float(row.get("Shares")),
                "ownership_type": "Direct" if row.get("Direct") == "Yes" else "Indirect",
                "is_derivative": False, "underlying_security": None,
                "exercise_price": None, "exercise_date": None,
                "expiration_date": None, "is_10b5_1_plan": None,
            })
    dt = getattr(form3, "derivative_table", None)
    if dt and not dt.holdings.empty:
        for _, row in dt.holdings.data.iterrows():
            rows.append({**base,
                "transaction_date": None, "security_title": row.get("Security"),
                "transaction_code": None, "transaction_type": "Initial Ownership",
                "acquired_disposed": "A",
                "shares": _safe_float(row.get("UnderlyingShares")),
                "price": None, "value": None,
                "shares_remaining": _safe_float(row.get("UnderlyingShares")),
                "ownership_type": _ownership_str(row.get("DirectIndirect", ""),
                                                 row.get("Nature Of Ownership", "") or ""),
                "is_derivative": True,
                "underlying_security": row.get("Underlying"),
                "exercise_price": _safe_float(row.get("ExercisePrice")),
                "exercise_date": row.get("ExerciseDate"),
                "expiration_date": row.get("ExpirationDate"),
                "is_10b5_1_plan": None,
            })
    return rows


def _rows_from_nd_transactions(form_obj, base: dict) -> list[dict]:
    rows = []
    nd_table = getattr(form_obj, "non_derivative_table", None)
    if not nd_table:
        return rows
    nd_txns = getattr(nd_table, "transactions", None)
    if not nd_txns or nd_txns.empty:
        return rows
    for _, row in nd_txns.data.iterrows():
        shares = _safe_float(row.get("Shares"))
        price  = _safe_float(row.get("Price"))
        value  = (shares * price) if (shares is not None and price is not None) else None
        rows.append({**base,
            "transaction_date": row.get("Date"),
            "security_title": row.get("Security"),
            "transaction_code": row.get("Code"),
            "transaction_type": row.get("TransactionType"),
            "acquired_disposed": row.get("AcquiredDisposed"),
            "shares": shares, "price": price, "value": value,
            "shares_remaining": _safe_float(row.get("Remaining")),
            "ownership_type": _ownership_str(row.get("DirectIndirect", ""),
                                             row.get("NatureOfOwnership", "") or ""),
            "is_derivative": False, "underlying_security": None,
            "exercise_price": None, "exercise_date": None,
            "expiration_date": None, "is_10b5_1_plan": None,
        })
    return rows


def _rows_from_d_transactions(form_obj, base: dict) -> list[dict]:
    rows = []
    d_table = getattr(form_obj, "derivative_table", None)
    if not d_table:
        return rows
    d_txns = getattr(d_table, "transactions", None)
    if not d_txns or d_txns.empty:
        return rows
    try:
        from edgar.ownership.ownershipforms import TransactionCode
        type_map = TransactionCode.TRANSACTION_TYPES
    except Exception:
        type_map = {}
    for _, row in d_txns.data.iterrows():
        shares = _safe_float(row.get("Shares"))
        price  = _safe_float(row.get("Price"))
        value  = (shares * price) if (shares is not None and price is not None) else None
        code   = row.get("Code", "")
        rows.append({**base,
            "transaction_date": row.get("Date"),
            "security_title": row.get("Security"),
            "transaction_code": code,
            "transaction_type": type_map.get(code, code),
            "acquired_disposed": row.get("AcquiredDisposed"),
            "shares": shares, "price": price, "value": value,
            "shares_remaining": _safe_float(row.get("Remaining")),
            "ownership_type": _ownership_str(row.get("DirectIndirect", ""), ""),
            "is_derivative": True,
            "underlying_security": row.get("Underlying"),
            "exercise_price": _safe_float(row.get("ExercisePrice")),
            "exercise_date": row.get("ExerciseDate"),
            "expiration_date": row.get("ExpirationDate"),
            "is_10b5_1_plan": None,
        })
    return rows


def _parse_section16_filing(filing, ticker: str, company_name: str) -> list[dict]:
    try:
        form_obj = filing.obj()
    except Exception as exc:
        print(f"    WARN: Could not parse {filing.accession_no}: {exc}")
        return []
    if form_obj is None:
        return []
    form_type = filing.form.replace("/A", "").strip()
    if form_type == "3":
        return _rows_from_form3(form_obj, ticker, company_name, filing)
    owners_meta = _get_owners_meta(form_obj)
    base = dict(
        ticker=ticker, company_name=company_name,
        filing_date=filing.filing_date, accession_no=filing.accession_no,
        filing_form=filing.form, **owners_meta
    )
    return _rows_from_nd_transactions(form_obj, base) + _rows_from_d_transactions(form_obj, base)


# ---------------------------------------------------------------------------
# Public API — called by pipeline.py
# ---------------------------------------------------------------------------

def upsert_section16(
    companies: list[tuple[str, str]],
    start_date: str,
    end_date: str,
    forms: tuple = ("3", "4", "5"),
    verbose: bool = True,
) -> int:
    """
    Fetch Section 16 filings and upsert into the database.
    Returns the count of newly inserted rows.
    """
    import pandas as pd
    from edgar import set_identity
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    set_identity(EDGAR_IDENTITY)
    date_range = f"{start_date}:{end_date}"
    session = get_session()
    new_rows = 0

    try:
        for ticker, display_name in companies:
            # Ensure company row exists
            if not session.get(Company, ticker):
                ec = _resolve_company(ticker, verbose=False)
                session.merge(Company(
                    ticker=ticker,
                    name=ec.name if ec else display_name,
                    cik=str(ec.cik) if ec else None,
                ))
                session.commit()

            ec = _resolve_company(ticker, verbose)
            if ec is None:
                continue
            if verbose:
                print(f"\n  {ticker} — {display_name}  (CIK: {ec.cik})")

            for form_type in forms:
                try:
                    filings = ec.get_filings(form=form_type, filing_date=date_range)
                except Exception as exc:
                    print(f"  WARN Form {form_type} for {ticker}: {exc}")
                    continue

                n = len(filings)
                if verbose:
                    print(f"    Form {form_type}: {n} filings")
                if n == 0:
                    continue

                for i, filing in enumerate(filings):
                    # Skip accession_nos we already have
                    accno = filing.accession_no
                    exists = session.query(Section16Filing).filter_by(
                        accession_no=accno
                    ).first()
                    if exists:
                        continue

                    rows = _parse_section16_filing(filing, ticker, display_name)
                    for r in rows:
                        # Coerce dates
                        for date_col in ("filing_date", "transaction_date",
                                         "exercise_date", "expiration_date"):
                            val = r.get(date_col)
                            if val is not None:
                                r[date_col] = pd.to_datetime(val, errors="coerce")
                                if pd.isna(r[date_col]):
                                    r[date_col] = None
                                else:
                                    r[date_col] = r[date_col].date()
                        obj = Section16Filing(**{
                            k: v for k, v in r.items()
                            if hasattr(Section16Filing, k)
                        })
                        session.add(obj)
                        new_rows += 1

                    if i % 50 == 49:
                        session.commit()

                session.commit()

    except Exception as exc:
        session.rollback()
        raise
    finally:
        session.close()

    return new_rows


def upsert_large_holder_stakes(
    companies: list[tuple[str, str]],
    start_date: str,
    end_date: str,
    verbose: bool = True,
) -> int:
    """
    Fetch Schedule 13D/13G filings and upsert into the database.
    Returns count of newly inserted rows.
    """
    import pandas as pd
    from edgar import set_identity

    set_identity(EDGAR_IDENTITY)
    date_range = f"{start_date}:{end_date}"
    session = get_session()
    new_rows = 0

    _13D = ["SCHEDULE 13D", "SC 13D/A"]
    _13G = ["SCHEDULE 13G", "SC 13G/A"]

    try:
        for ticker, display_name in companies:
            ec = _resolve_company(ticker, verbose)
            if ec is None:
                continue

            for form_group in (_13D, _13G):
                for form_name in form_group:
                    try:
                        filings = ec.get_filings(form=form_name, filing_date=date_range)
                    except Exception:
                        continue

                    for filing in filings:
                        accno = filing.accession_no
                        if session.query(LargeHolderStake).filter_by(
                            accession_no=accno
                        ).first():
                            continue

                        try:
                            schedule = filing.obj()
                        except Exception as exc:
                            print(f"    WARN: {accno}: {exc}")
                            continue
                        if schedule is None:
                            continue

                        is_activist = filing.form.upper().replace("/A", "").strip() in (
                            "SCHEDULE 13D", "SC 13D"
                        )
                        for person in schedule.reporting_persons:
                            row = LargeHolderStake(
                                accession_no=accno,
                                ticker=ticker,
                                company_name=display_name,
                                filing_date=filing.filing_date,
                                filing_form=filing.form,
                                date_of_event=getattr(schedule, "date_of_event", None),
                                amendment_number=getattr(schedule, "amendment_number", None),
                                is_activist=is_activist,
                                holder_name=person.name,
                                holder_cik=person.cik or None,
                                holder_type=person.type_of_reporting_person,
                                holder_citizenship=person.citizenship or None,
                                fund_type=person.fund_type or None,
                                is_group_member=(person.member_of_group == "a"),
                                aggregate_shares=person.aggregate_amount,
                                percent_of_class=person.percent_of_class,
                                sole_voting_power=person.sole_voting_power,
                                shared_voting_power=person.shared_voting_power,
                                sole_dispositive_power=person.sole_dispositive_power,
                                shared_dispositive_power=person.shared_dispositive_power,
                                purpose_of_transaction=(
                                    getattr(schedule.items, "item4_purpose_of_transaction", None)
                                    if is_activist else None
                                ),
                                source_of_funds=(
                                    getattr(schedule.items, "item3_source_of_funds", None)
                                    if is_activist else None
                                ),
                                issuer_name=(
                                    schedule.issuer_info.name
                                    if schedule.issuer_info else None
                                ),
                            )
                            session.add(row)
                            new_rows += 1

                session.commit()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return new_rows

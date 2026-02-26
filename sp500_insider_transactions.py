"""
sp500_insider_transactions.py

Captures every "key person" disclosure for the top 10 S&P 500 companies
over a given date range, across THREE filing categories:

  1. SECTION 16 FILINGS  (Forms 3, 4, 5)
     Who:  Officers, directors, 10%+ owners
     What: - Form 3 → initial holdings when appointed
           - Form 4 → every buy/sell/award/exercise (within 2 business days)
           - Form 5 → annual catch-up for exempt/missed transactions
     Output file: section16_filings.csv

  2. LARGE HOLDER STAKES  (Schedule 13D, 13G)
     Who:  Anyone who crosses 5% ownership (not just insiders)
           Includes activist hedge funds, institutional investors, etc.
     What: - 13D → active/activist ownership (potential control intent)
           - 13G → passive ownership (Vanguard, BlackRock, index funds etc.)
     Output file: large_holder_stakes.csv

  NOT captured (requires different data):
  - Institutional investors below 5%  → 13F quarterly holdings reports
  - Employees below Section 16 status → no SEC filing obligation
  - Hedge funds below 5% ownership    → no beneficial ownership filing

Companies (GOOGL and GOOG share the same CIK, so 9 unique entities):
  NVDA, AAPL, MSFT, AMZN, GOOGL, META, TSLA, AVGO, BRK-B

Usage:
    python sp500_insider_transactions.py

    Or import and call:
        from sp500_insider_transactions import fetch_all
        section16_df, stakes_df = fetch_all()
"""

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EDGAR_IDENTITY = "r63848@proton.me"

START_DATE = "2023-01-01"
END_DATE   = "2026-02-26"

# GOOGL and GOOG are the same entity (Alphabet, CIK 0001652044) — one entry.
COMPANIES = [
    # ("NVDA",  "Nvidia"),
    # ("AAPL",  "Apple Inc."),
    ("CORZ", "Core Scientific, Inc.")
    # ("MSFT",  "Microsoft"),
    # ("AMZN",  "Amazon"),
    # ("GOOGL", "Alphabet Inc."),
    # ("META",  "Meta Platforms"),
    # ("TSLA",  "Tesla"),
    # ("AVGO",  "Broadcom"),
    # ("BRK-B", "Berkshire Hathaway"),
]

OUTPUT_SECTION16  = "section16_filings.csv"
OUTPUT_LARGE_HOLDER = "large_holder_stakes.csv"


# ===========================================================================
# SECTION 16 helpers  (Forms 3, 4, 5)
# ===========================================================================

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        import math
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
    """Primary reporting owner metadata (works for Form 3/4/5)."""
    owners = getattr(form_obj, "reporting_owners", None)
    if not owners or len(owners) == 0:
        return dict(insider_name=None, insider_cik=None, is_director=None,
                    is_officer=None, is_ten_pct_owner=None,
                    officer_title=None, position=None)
    names     = " / ".join(o.name      for o in owners.owners if o.name)
    ciks      = " / ".join(str(o.cik)  for o in owners.owners if o.cik)
    titles    = " / ".join(o.officer_title for o in owners.owners if o.officer_title)
    positions = " / ".join(o.position  for o in owners.owners if o.position)
    return dict(
        insider_name     = names or None,
        insider_cik      = ciks  or None,
        is_director      = any(o.is_director      for o in owners.owners),
        is_officer       = any(o.is_officer        for o in owners.owners),
        is_ten_pct_owner = any(o.is_ten_pct_owner  for o in owners.owners),
        officer_title    = titles    or None,
        position         = positions or None,
    )


def _rows_from_form3(form3, filing_meta: dict) -> list[dict]:
    """Form 3 = initial holdings snapshot (no transactions, just positions)."""
    rows = []
    owners_meta = _get_owners_meta(form3)
    base = {**filing_meta, **owners_meta}

    # Non-derivative holdings (common stock)
    nd = getattr(form3, "non_derivative_table", None)
    if nd and not nd.holdings.empty:
        for _, row in nd.holdings.data.iterrows():
            rows.append({
                **base,
                "transaction_date":     None,
                "security_title":       row.get("Security"),
                "transaction_code":     None,
                "transaction_type":     "Initial Ownership",
                "acquired_disposed":    "A",
                "shares":               _safe_float(row.get("Shares")),
                "price":                None,
                "value":                None,
                "shares_remaining":     _safe_float(row.get("Shares")),
                "ownership_type":       "Direct" if row.get("Direct") == "Yes" else "Indirect",
                "is_derivative":        False,
                "underlying_security":  None,
                "exercise_price":       None,
                "exercise_date":        None,
                "expiration_date":      None,
                "is_10b5_1_plan":       None,
            })

    # Derivative holdings (options/warrants/RSUs)
    dt = getattr(form3, "derivative_table", None)
    if dt and not dt.holdings.empty:
        for _, row in dt.holdings.data.iterrows():
            rows.append({
                **base,
                "transaction_date":     None,
                "security_title":       row.get("Security"),
                "transaction_code":     None,
                "transaction_type":     "Initial Ownership",
                "acquired_disposed":    "A",
                "shares":               _safe_float(row.get("UnderlyingShares")),
                "price":                None,
                "value":                None,
                "shares_remaining":     _safe_float(row.get("UnderlyingShares")),
                "ownership_type":       _ownership_str(row.get("DirectIndirect", ""), row.get("Nature Of Ownership", "") or ""),
                "is_derivative":        True,
                "underlying_security":  row.get("Underlying"),
                "exercise_price":       _safe_float(row.get("ExercisePrice")),
                "exercise_date":        row.get("ExerciseDate"),
                "expiration_date":      row.get("ExpirationDate"),
                "is_10b5_1_plan":       None,
            })

    return rows


def _rows_from_nd_transactions(form_obj, filing_meta: dict, owners_meta: dict) -> list[dict]:
    """Non-derivative transactions from Form 4 or Form 5."""
    rows = []
    nd_table = getattr(form_obj, "non_derivative_table", None)
    if not nd_table:
        return rows
    nd_txns = getattr(nd_table, "transactions", None)
    if not nd_txns or nd_txns.empty:
        return rows

    base = {**filing_meta, **owners_meta}

    for _, row in nd_txns.data.iterrows():
        shares = _safe_float(row.get("Shares"))
        price  = _safe_float(row.get("Price"))
        value  = (shares * price) if (shares is not None and price is not None) else None

        footnote_ids   = row.get("footnotes", "") or ""
        footnotes_text = ""
        if footnote_ids and hasattr(form_obj, "_resolve_footnotes"):
            footnotes_text = form_obj._resolve_footnotes(footnote_ids)

        from edgar.ownership.core import detect_10b5_1_plan
        is_10b5_1 = detect_10b5_1_plan(footnotes_text) if footnotes_text else None

        rows.append({
            **base,
            "transaction_date":     row.get("Date"),
            "security_title":       row.get("Security"),
            "transaction_code":     row.get("Code"),
            "transaction_type":     row.get("TransactionType"),
            "acquired_disposed":    row.get("AcquiredDisposed"),
            "shares":               shares,
            "price":                price,
            "value":                value,
            "shares_remaining":     _safe_float(row.get("Remaining")),
            "ownership_type":       _ownership_str(row.get("DirectIndirect", ""), row.get("NatureOfOwnership", "") or ""),
            "is_derivative":        False,
            "underlying_security":  None,
            "exercise_price":       None,
            "exercise_date":        None,
            "expiration_date":      None,
            "is_10b5_1_plan":       is_10b5_1,
        })
    return rows


def _rows_from_d_transactions(form_obj, filing_meta: dict, owners_meta: dict) -> list[dict]:
    """Derivative transactions from Form 4 or Form 5."""
    rows = []
    d_table = getattr(form_obj, "derivative_table", None)
    if not d_table:
        return rows
    d_txns = getattr(d_table, "transactions", None)
    if not d_txns or d_txns.empty:
        return rows

    base = {**filing_meta, **owners_meta}

    from edgar.ownership.ownershipforms import TransactionCode
    for _, row in d_txns.data.iterrows():
        shares = _safe_float(row.get("Shares"))
        price  = _safe_float(row.get("Price"))
        value  = (shares * price) if (shares is not None and price is not None) else None
        code   = row.get("Code", "")

        rows.append({
            **base,
            "transaction_date":     row.get("Date"),
            "security_title":       row.get("Security"),
            "transaction_code":     code,
            "transaction_type":     TransactionCode.TRANSACTION_TYPES.get(code, code),
            "acquired_disposed":    row.get("AcquiredDisposed"),
            "shares":               shares,
            "price":                price,
            "value":                value,
            "shares_remaining":     _safe_float(row.get("Remaining")),
            "ownership_type":       _ownership_str(row.get("DirectIndirect", ""), ""),
            "is_derivative":        True,
            "underlying_security":  row.get("Underlying"),
            "exercise_price":       _safe_float(row.get("ExercisePrice")),
            "exercise_date":        row.get("ExerciseDate"),
            "expiration_date":      row.get("ExpirationDate"),
            "is_10b5_1_plan":       None,
        })
    return rows


def _parse_section16_filing(filing, filing_meta: dict) -> list[dict]:
    """Dispatch to correct parser based on form type."""
    try:
        form_obj = filing.obj()
    except Exception as exc:
        print(f"    WARN: Could not parse {filing.accession_no}: {exc}")
        return []

    if form_obj is None:
        return []

    form_type = filing.form.replace("/A", "").strip()

    if form_type == "3":
        return _rows_from_form3(form_obj, filing_meta)

    # Form 4 or Form 5 — transaction-based
    owners_meta = _get_owners_meta(form_obj)
    rows  = _rows_from_nd_transactions(form_obj, filing_meta, owners_meta)
    rows += _rows_from_d_transactions(form_obj,  filing_meta, owners_meta)
    return rows


def fetch_section16_filings(
    companies=COMPANIES,
    start_date: str = START_DATE,
    end_date: str   = END_DATE,
    forms: tuple    = ("3", "4", "5"),
    verbose: bool   = True,
) -> pd.DataFrame:
    """
    Fetch Section 16 filings (Forms 3, 4, 5) for the given companies.

    Each row = one transaction (Form 4/5) or one holding line (Form 3).

    Key columns:
      filing_form        — "3", "4", "4/A", "5", "5/A"
      transaction_type   — Purchase / Sale / Award / Exercise / Tax / Initial Ownership …
      transaction_code   — P S A D F M G C X W Z (raw SEC code)
      acquired_disposed  — A (acquired/bought) or D (disposed/sold)
      is_derivative      — False=common stock, True=option/RSU/warrant
      is_10b5_1_plan     — True if pre-planned 10b5-1 trading plan cited in footnotes
    """
    from edgar import Company, set_identity
    from edgar.entity import CompanyNotFoundError

    set_identity(EDGAR_IDENTITY)
    date_range = f"{start_date}:{end_date}"
    all_rows: list[dict] = []

    for ticker, company_display_name in companies:
        if verbose:
            print(f"\n{'='*60}")
            print(f"  {ticker} — {company_display_name}")
            print(f"{'='*60}")

        # Resolve company
        company = _resolve_company(ticker, verbose)
        if company is None:
            continue

        if verbose:
            print(f"  CIK: {company.cik}  |  Name: {company.name}")

        for form_type in forms:
            try:
                filings = company.get_filings(form=form_type, filing_date=date_range)
            except Exception as exc:
                print(f"  ERROR fetching Form {form_type} for {ticker}: {exc}")
                continue

            n = len(filings)
            if verbose:
                print(f"  Form {form_type}: {n} filings found")
            if n == 0:
                continue

            parsed = errors = empty = 0
            for i, filing in enumerate(filings):
                if verbose and i > 0 and i % 25 == 0:
                    print(f"    ... processed {i}/{n}")

                filing_meta = {
                    "ticker":        ticker,
                    "company_name":  company_display_name,
                    "filing_date":   str(filing.filing_date),
                    "accession_no":  filing.accession_no,
                    "filing_form":   filing.form,
                }

                rows = _parse_section16_filing(filing, filing_meta)
                if rows:
                    all_rows.extend(rows)
                    parsed += 1
                else:
                    empty += 1

            if verbose:
                print(f"    → {parsed} filings with data, {empty} empty/skipped")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["filing_date"]      = pd.to_datetime(df["filing_date"],     errors="coerce").dt.date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
    for col in ["shares", "price", "value", "shares_remaining", "exercise_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values(
        ["ticker", "filing_date", "insider_name", "transaction_date"],
        ignore_index=True,
    )


# ===========================================================================
# LARGE HOLDER helpers  (Schedule 13D / 13G)
# ===========================================================================

# edgartools form names for 13D/13G (including amendments)
_13D_FORMS = ["SCHEDULE 13D", "SC 13D", "SC 13D/A", "SCHEDULE 13D/A"]
_13G_FORMS = ["SCHEDULE 13G", "SC 13G", "SC 13G/A", "SCHEDULE 13G/A"]


def _parse_13d_or_13g(filing, ticker: str, company_display_name: str) -> list[dict]:
    """
    Parse a Schedule 13D or 13G filing.
    Returns one row per reporting person (joint filers produce multiple rows).
    """
    try:
        schedule = filing.obj()
    except Exception as exc:
        print(f"    WARN: Could not parse {filing.accession_no}: {exc}")
        return []

    if schedule is None:
        return []

    is_activist = filing.form.upper().replace("/A", "").strip() in ("SCHEDULE 13D", "SC 13D")

    rows = []
    for person in schedule.reporting_persons:
        rows.append({
            # Filing identity
            "ticker":              ticker,
            "company_name":        company_display_name,
            "filing_date":         str(filing.filing_date),
            "accession_no":        filing.accession_no,
            "filing_form":         filing.form,
            "date_of_event":       getattr(schedule, "date_of_event", None),
            "amendment_number":    getattr(schedule, "amendment_number", None),
            "is_activist":         is_activist,

            # Reporting person
            "holder_name":         person.name,
            "holder_cik":          person.cik or None,
            "holder_type":         person.type_of_reporting_person,
            "holder_citizenship":  person.citizenship or None,
            "fund_type":           person.fund_type or None,
            "is_group_member":     person.member_of_group == "a",

            # Ownership position
            "aggregate_shares":         person.aggregate_amount,
            "percent_of_class":         person.percent_of_class,
            "sole_voting_power":        person.sole_voting_power,
            "shared_voting_power":      person.shared_voting_power,
            "sole_dispositive_power":   person.sole_dispositive_power,
            "shared_dispositive_power": person.shared_dispositive_power,

            # 13D-specific narrative (None for 13G)
            "purpose_of_transaction": (
                getattr(schedule.items, "item4_purpose_of_transaction", None)
                if is_activist else None
            ),
            "source_of_funds": (
                getattr(schedule.items, "item3_source_of_funds", None)
                if is_activist else None
            ),

            # Issuer info (cross-check)
            "issuer_name": schedule.issuer_info.name if schedule.issuer_info else None,
        })

    return rows


def fetch_large_holder_filings(
    companies=COMPANIES,
    start_date: str = START_DATE,
    end_date: str   = END_DATE,
    verbose: bool   = True,
) -> pd.DataFrame:
    """
    Fetch Schedule 13D and 13G large holder disclosures for the given companies.

    Each row = one reporting person in one 13D/13G filing.
    Joint filers produce multiple rows per filing.

    Key columns:
      is_activist            — True=13D (active/activist), False=13G (passive)
      amendment_number       — None=initial filing, 1/2/3…=amendment
      percent_of_class       — % of company shares owned
      aggregate_shares       — total shares beneficially owned
      purpose_of_transaction — 13D Item 4 narrative (activist intent text)
      holder_type            — IA=Investment Adviser, HC=Holding Company,
                               IN=Individual, OO=Other, etc.
    """
    from edgar import Company, set_identity
    from edgar.entity import CompanyNotFoundError

    set_identity(EDGAR_IDENTITY)
    date_range = f"{start_date}:{end_date}"
    all_rows: list[dict] = []

    for ticker, company_display_name in companies:
        if verbose:
            print(f"\n{'='*60}")
            print(f"  {ticker} — {company_display_name}  (13D/13G)")
            print(f"{'='*60}")

        company = _resolve_company(ticker, verbose)
        if company is None:
            continue

        if verbose:
            print(f"  CIK: {company.cik}  |  Name: {company.name}")

        for form_group, label in [
            (["SCHEDULE 13D", "SC 13D/A"], "13D (activist)"),
            (["SCHEDULE 13G", "SC 13G/A"], "13G (passive)"),
        ]:
            filing_rows: list[dict] = []
            for form_name in form_group:
                try:
                    filings = company.get_filings(form=form_name, filing_date=date_range)
                except Exception as exc:
                    # Form not found is normal — many companies have no 13D/13G
                    continue

                for filing in filings:
                    rows = _parse_13d_or_13g(filing, ticker, company_display_name)
                    filing_rows.extend(rows)

            if verbose:
                n_filings = len(set(r["accession_no"] for r in filing_rows))
                print(f"  {label}: {n_filings} filings → {len(filing_rows)} person-rows")

            all_rows.extend(filing_rows)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["filing_date"]   = pd.to_datetime(df["filing_date"],  errors="coerce").dt.date
    df["date_of_event"] = pd.to_datetime(df["date_of_event"], errors="coerce").dt.date
    for col in ["aggregate_shares", "percent_of_class",
                "sole_voting_power", "shared_voting_power",
                "sole_dispositive_power", "shared_dispositive_power"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values(
        ["ticker", "filing_date", "holder_name"],
        ignore_index=True,
    )


# ===========================================================================
# Shared helpers
# ===========================================================================

def _resolve_company(ticker: str, verbose: bool):
    """Resolve ticker to Company, trying BRK-B/BRK.B alternatives."""
    from edgar import Company
    from edgar.entity import CompanyNotFoundError

    try:
        return Company(ticker)
    except CompanyNotFoundError:
        alt = {"BRK-B": "BRK.B", "BRK.B": "BRK-B"}.get(ticker)
        if alt:
            try:
                c = Company(alt)
                if verbose:
                    print(f"  (resolved via alternate ticker {alt})")
                return c
            except CompanyNotFoundError:
                pass
        print(f"  ERROR: Could not find company for ticker {ticker} — skipping.")
        return None


# ===========================================================================
# Summary printers
# ===========================================================================

def print_section16_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("Section 16 DataFrame is empty.")
        return
    print("\n" + "="*70)
    print("SECTION 16 SUMMARY  (Forms 3, 4, 5)")
    print("="*70)
    print(f"Total rows      : {len(df):,}")
    print(f"Unique filings  : {df['accession_no'].nunique():,}")
    print(f"Unique insiders : {df['insider_name'].nunique():,}")
    print(f"Date range      : {df['filing_date'].min()}  →  {df['filing_date'].max()}")
    print()

    print("Rows per company / form:")
    pt = df.groupby(["ticker", "filing_form"]).size().unstack(fill_value=0)
    print(pt.to_string())
    print()

    print("Transaction type breakdown:")
    breakdown = (
        df.groupby("transaction_type").size()
          .sort_values(ascending=False)
          .head(15)
    )
    for ttype, count in breakdown.items():
        print(f"  {ttype:<30} {count:>5}")

    market = df[df["transaction_code"].isin(["P", "S"])]
    if not market.empty:
        print()
        print("Open-market trades only (P=Purchase / S=Sale):")
        for ticker, grp in market.groupby("ticker"):
            buys  = grp[grp.acquired_disposed == "A"]["value"].sum()
            sells = grp[grp.acquired_disposed == "D"]["value"].sum()
            net   = buys - sells
            print(f"  {ticker:<8} bought ${buys:>14,.0f}  sold ${sells:>14,.0f}  net ${net:>+14,.0f}")
    print("="*70)


def print_large_holder_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("Large Holder DataFrame is empty.")
        return
    print("\n" + "="*70)
    print("LARGE HOLDER SUMMARY  (Schedule 13D / 13G)")
    print("="*70)
    print(f"Total rows        : {len(df):,}")
    print(f"Unique filings    : {df['accession_no'].nunique():,}")
    print(f"Unique holders    : {df['holder_name'].nunique():,}")
    print(f"Activist filings  : {df['is_activist'].sum():,}")
    print(f"Passive filings   : {(~df['is_activist']).sum():,}")
    print()

    print("Positions ≥5% by company:")
    top = (
        df.groupby(["ticker", "holder_name", "is_activist"])["percent_of_class"]
          .max()
          .reset_index()
          .sort_values(["ticker", "percent_of_class"], ascending=[True, False])
    )
    for _, row in top.iterrows():
        flag = "ACTIVIST" if row.is_activist else "passive "
        print(f"  {row.ticker:<8} {flag}  {row.percent_of_class:>5.1f}%  {row.holder_name}")

    activists = df[df.is_activist & df.purpose_of_transaction.notna()]
    if not activists.empty:
        print()
        print("Activist filings with stated purpose:")
        for _, row in activists.iterrows():
            print(f"\n  [{row.ticker}] {row.holder_name}  filed {row.filing_date}")
            purpose = str(row.purpose_of_transaction)
            print(f"  Purpose: {purpose[:300]}{'...' if len(purpose) > 300 else ''}")
    print("="*70)


# ===========================================================================
# Top-level convenience function
# ===========================================================================

def fetch_all(
    companies=COMPANIES,
    start_date: str = START_DATE,
    end_date: str   = END_DATE,
    verbose: bool   = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch both Section 16 filings and large holder stakes.

    Returns:
        (section16_df, large_holder_df)
    """
    print("\n" + "#"*70)
    print("# STEP 1 / 2 — Section 16 filings (Forms 3, 4, 5)")
    print("#"*70)
    section16_df = fetch_section16_filings(
        companies=companies, start_date=start_date, end_date=end_date, verbose=verbose
    )

    print("\n" + "#"*70)
    print("# STEP 2 / 2 — Large holder stakes (Schedule 13D / 13G)")
    print("#"*70)
    stakes_df = fetch_large_holder_filings(
        companies=companies, start_date=start_date, end_date=end_date, verbose=verbose
    )

    return section16_df, stakes_df


# ===========================================================================
# Main
# ===========================================================================

if __name__ == "__main__":
    print(f"Date range : {START_DATE} → {END_DATE}")
    print(f"Companies  : {', '.join(t for t, _ in COMPANIES)}")

    section16_df, stakes_df = fetch_all(verbose=True)

    # ── Save Section 16 ──────────────────────────────────────────────────────
    if not section16_df.empty:
        out = Path(OUTPUT_SECTION16)
        section16_df.to_csv(out, index=False)
        print(f"\nSaved {len(section16_df):,} rows → {out.resolve()}")
        print_section16_summary(section16_df)

        print("\nFirst 5 rows (key columns):")
        cols = ["ticker", "filing_form", "filing_date", "insider_name",
                "officer_title", "transaction_date", "transaction_type",
                "shares", "price", "value"]
        print(section16_df[cols].head(5).to_string(index=False))
    else:
        print("\nNo Section 16 data found.")

    # ── Save Large Holder ────────────────────────────────────────────────────
    if not stakes_df.empty:
        out2 = Path(OUTPUT_LARGE_HOLDER)
        stakes_df.to_csv(out2, index=False)
        print(f"\nSaved {len(stakes_df):,} rows → {out2.resolve()}")
        print_large_holder_summary(stakes_df)
    else:
        print("\nNo large holder filings found in this period.")

    if section16_df.empty and stakes_df.empty:
        sys.exit(1)

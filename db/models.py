"""
db/models.py — SQLAlchemy ORM models for CeoWatcher.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, Float,
    ForeignKey, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# companies
# ---------------------------------------------------------------------------
class Company(Base):
    __tablename__ = "companies"

    ticker       = Column(String(10), primary_key=True)
    name         = Column(String(255))
    cik          = Column(String(20))
    sector       = Column(String(100))
    added_at     = Column(DateTime, default=datetime.utcnow)

    filings      = relationship("Section16Filing",  back_populates="company_ref", lazy="dynamic")
    stakes       = relationship("LargeHolderStake", back_populates="company_ref", lazy="dynamic")
    prices       = relationship("PriceHistory",     back_populates="company_ref", lazy="dynamic")
    analytics    = relationship("InsiderAnalytics", back_populates="company_ref", lazy="dynamic")


# ---------------------------------------------------------------------------
# section16_filings  — one row per Form 3/4/5 transaction line
# ---------------------------------------------------------------------------
class Section16Filing(Base):
    __tablename__ = "section16_filings"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    accession_no       = Column(String(50), nullable=False, index=True)
    ticker             = Column(String(10), ForeignKey("companies.ticker"), index=True)
    company_name       = Column(String(255))
    filing_form        = Column(String(10), index=True)
    filing_date        = Column(Date, index=True)
    insider_name       = Column(Text, index=True)
    insider_cik        = Column(Text, index=True)
    is_director        = Column(Boolean)
    is_officer         = Column(Boolean)
    is_ten_pct_owner   = Column(Boolean)
    officer_title      = Column(String(255))
    position           = Column(String(255))
    transaction_date   = Column(Date, index=True)
    security_title     = Column(String(255))
    transaction_code   = Column(String(5), index=True)
    transaction_type   = Column(Text)
    acquired_disposed  = Column(String(1))
    shares             = Column(Float)
    price              = Column(Float)
    value              = Column(Float)
    shares_remaining   = Column(Float)
    ownership_type     = Column(Text)
    is_derivative      = Column(Boolean, default=False)
    underlying_security = Column(String(255))
    exercise_price     = Column(Float)
    exercise_date      = Column(Date)
    expiration_date    = Column(Date)
    is_10b5_1_plan     = Column(Boolean)
    created_at         = Column(DateTime, default=datetime.utcnow)

    company_ref = relationship("Company", back_populates="filings")


# ---------------------------------------------------------------------------
# large_holder_stakes  — Schedule 13D / 13G
# ---------------------------------------------------------------------------
class LargeHolderStake(Base):
    __tablename__ = "large_holder_stakes"

    id                      = Column(Integer, primary_key=True, autoincrement=True)
    accession_no            = Column(String(50), nullable=False, index=True)
    ticker                  = Column(String(10), ForeignKey("companies.ticker"), index=True)
    company_name            = Column(String(255))
    filing_date             = Column(Date, index=True)
    filing_form             = Column(String(30))
    date_of_event           = Column(Date)
    amendment_number        = Column(Integer)
    is_activist             = Column(Boolean)
    holder_name             = Column(String(255), index=True)
    holder_cik              = Column(String(20))
    holder_type             = Column(String(50))
    holder_citizenship      = Column(String(100))
    fund_type               = Column(String(50))
    is_group_member         = Column(Boolean)
    aggregate_shares        = Column(Float)
    percent_of_class        = Column(Float)
    sole_voting_power       = Column(Float)
    shared_voting_power     = Column(Float)
    sole_dispositive_power  = Column(Float)
    shared_dispositive_power = Column(Float)
    purpose_of_transaction  = Column(Text)
    source_of_funds         = Column(Text)
    issuer_name             = Column(String(255))
    created_at              = Column(DateTime, default=datetime.utcnow)

    company_ref = relationship("Company", back_populates="stakes")


# ---------------------------------------------------------------------------
# price_history  — daily adjusted close per ticker
# ---------------------------------------------------------------------------
class PriceHistory(Base):
    __tablename__ = "price_history"
    __table_args__ = (UniqueConstraint("ticker", "date"),)

    id     = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), ForeignKey("companies.ticker"), index=True)
    date   = Column(Date, index=True)
    close  = Column(Float, nullable=False)

    company_ref = relationship("Company", back_populates="prices")


# ---------------------------------------------------------------------------
# insider_analytics  — pre-computed per (ticker, insider_name)
# ---------------------------------------------------------------------------
class InsiderAnalytics(Base):
    __tablename__ = "insider_analytics"
    __table_args__ = (UniqueConstraint("ticker", "insider_name"),)

    id                       = Column(Integer, primary_key=True, autoincrement=True)
    ticker                   = Column(String(10), ForeignKey("companies.ticker"), index=True)
    insider_name             = Column(Text, index=True)
    insider_cik              = Column(Text)
    company_name             = Column(String(255))
    officer_title            = Column(String(255))
    is_director              = Column(Boolean)
    is_officer               = Column(Boolean)
    is_ten_pct_owner         = Column(Boolean)

    # Timeline
    first_txn_date           = Column(Date)
    last_filing_date         = Column(Date)

    # Stock since entry
    entry_price              = Column(Float)
    current_price            = Column(Float)
    stock_pct_since_entry    = Column(Float)

    # Return windows: stock % change in N days after first_txn_date
    pct_2w                   = Column(Float)
    pct_1m                   = Column(Float)
    pct_3m                   = Column(Float)
    pct_6m                   = Column(Float)
    pct_1y                   = Column(Float)
    pct_2y                   = Column(Float)
    pct_3y                   = Column(Float)

    # Position
    last_reported_shares     = Column(Float)
    current_position_value   = Column(Float)

    # Open-market purchases (Code=P)
    n_open_mkt_buys          = Column(Integer)
    open_mkt_shares_bought   = Column(Float)
    open_mkt_total_cost      = Column(Float)
    open_mkt_wacb            = Column(Float)
    open_mkt_unrealized_pct  = Column(Float)
    open_mkt_unrealized_usd  = Column(Float)

    # Open-market sales (Code=S)
    n_open_mkt_sells         = Column(Integer)
    open_mkt_shares_sold     = Column(Float)
    open_mkt_total_proceeds  = Column(Float)
    open_mkt_avg_sell_price  = Column(Float)
    realized_pct             = Column(Float)

    # Awards (Code=A)
    shares_awarded           = Column(Float)
    award_current_value      = Column(Float)

    # Net conviction
    net_open_mkt_shares      = Column(Float)
    pct_trades_on_10b5_plan  = Column(Float)

    computed_at              = Column(DateTime, default=datetime.utcnow)

    company_ref = relationship("Company", back_populates="analytics")


# ---------------------------------------------------------------------------
# flags  — interesting filing alerts
# ---------------------------------------------------------------------------
class Flag(Base):
    __tablename__ = "flags"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    ticker       = Column(String(10), index=True)
    insider_name = Column(String(255))
    accession_no = Column(String(50), index=True)
    flag_type    = Column(String(100))        # e.g. "CLUSTER_BUY", "CEO_PURCHASE"
    severity     = Column(String(20))         # HIGH / MEDIUM / LOW
    description  = Column(Text)
    flagged_at   = Column(DateTime, default=datetime.utcnow)
    is_dismissed = Column(Boolean, default=False)


# ---------------------------------------------------------------------------
# ingest_runs  — audit log for the pipeline
# ---------------------------------------------------------------------------
class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    run_at              = Column(DateTime, default=datetime.utcnow)
    companies_processed = Column(Integer, default=0)
    new_section16_rows  = Column(Integer, default=0)
    new_stakes_rows     = Column(Integer, default=0)
    analytics_refreshed = Column(Integer, default=0)
    flags_raised        = Column(Integer, default=0)
    errors              = Column(Text)
    status              = Column(String(20))   # success / partial / failed

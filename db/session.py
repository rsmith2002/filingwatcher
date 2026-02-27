"""
db/session.py — database engine and session factory.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import DATABASE_URL
from db.models import Base

_engine = None
_SessionFactory = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,      # detect stale connections
            pool_size=5,
            max_overflow=10,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 30,   # wait up to 30s for Neon to wake
            },
        )
    return _engine


def get_session() -> Session:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine())
    return _SessionFactory()


def _drop_accession_unique_constraints():
    """
    One-time migration: SQLAlchemy created a UNIQUE INDEX named ix_<table>_accession_no
    (from index=True + unique=True on the column). Drop it and recreate as a plain
    (non-unique) index so multiple rows per filing can be stored.
    Safe to run repeatedly — all statements use IF EXISTS / IF NOT EXISTS.
    """
    from sqlalchemy import inspect, text
    engine = get_engine()

    # Only act on tables that actually exist (skip on a fresh empty DB)
    existing = inspect(engine).get_table_names()

    ops = [
        ("section16_filings",  "ix_section16_filings_accession_no"),
        ("large_holder_stakes","ix_large_holder_stakes_accession_no"),
    ]
    with engine.connect() as conn:
        for table, idx in ops:
            if table not in existing:
                continue
            try:
                # Drop the unique index
                conn.execute(text(f"DROP INDEX IF EXISTS {idx}"))
                conn.commit()
                # Recreate as plain (non-unique) index for query performance
                conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS {idx} ON {table} (accession_no)"
                ))
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(f"  Migration note ({table}): {exc}")


def _widen_varchar_columns():
    """
    Convert insider_cik / insider_name from VARCHAR(20) → TEXT.
    VARCHAR → TEXT is a pure catalog change in PostgreSQL (no rewrite,
    no index rebuild needed), so it always succeeds.
    Safe to run repeatedly — altering TEXT→TEXT is a no-op.
    """
    from sqlalchemy import inspect, text
    engine = get_engine()
    existing = inspect(engine).get_table_names()
    print(f"  Migration: tables found = {existing}")

    col_changes = [
        ("section16_filings", "insider_cik"),
        ("section16_filings", "insider_name"),
        ("insider_analytics", "insider_cik"),
        ("insider_analytics", "insider_name"),
    ]
    with engine.connect() as conn:
        for table, col in col_changes:
            if table not in existing:
                print(f"  Migration: {table} not found, skipping")
                continue
            try:
                conn.execute(text(
                    f"ALTER TABLE {table} ALTER COLUMN {col} TYPE TEXT"
                ))
                conn.commit()
                print(f"  Migration: {table}.{col} → TEXT  OK")
            except Exception as exc:
                conn.rollback()
                print(f"  Migration FAILED ({table}.{col}): {exc}")


def init_db():
    """Create all tables if they don't exist. Safe to call repeatedly."""
    engine = get_engine()
    _drop_accession_unique_constraints()
    _widen_varchar_columns()
    Base.metadata.create_all(engine)
    print("Database tables verified / created.")

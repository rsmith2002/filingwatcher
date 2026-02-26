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
    One-time migration: remove unique constraints on accession_no so that
    multi-transaction filings (many rows, same accession_no) can be stored.
    Safe to run repeatedly — IF NOT EXISTS / DROP CONSTRAINT IF EXISTS is idempotent.
    """
    from sqlalchemy import text
    engine = get_engine()
    stmts = [
        "ALTER TABLE section16_filings DROP CONSTRAINT IF EXISTS section16_filings_accession_no_key",
        "ALTER TABLE large_holder_stakes DROP CONSTRAINT IF EXISTS large_holder_stakes_accession_no_key",
    ]
    with engine.connect() as conn:
        for stmt in stmts:
            try:
                conn.execute(text(stmt))
                conn.commit()
            except Exception:
                conn.rollback()


def init_db():
    """Create all tables if they don't exist. Safe to call repeatedly."""
    engine = get_engine()
    _drop_accession_unique_constraints()
    Base.metadata.create_all(engine)
    print("Database tables verified / created.")

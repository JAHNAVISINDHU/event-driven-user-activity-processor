from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from . import config

engine = create_engine(config.DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def init_db():
    """Create tables if they do not already exist (ORM-based migration).

    This is a safety net on top of init.sql, which is what actually runs
    automatically when the PostgreSQL container starts (mounted into
    /docker-entrypoint-initdb.d). Calling this again on app startup is a
    no-op if the tables already exist.
    """
    # Import models so they're registered on Base.metadata before create_all.
    from . import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

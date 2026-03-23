"""
db/connection.py
----------------
Single source of truth for database connections.
Provides both:
  - get_connection()  → raw pyodbc connection (for bulk inserts / stored procs)
  - get_engine()      → SQLAlchemy engine (for pandas read_sql / DataFrame inserts)
"""
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text
from loguru import logger
from config.settings import DBConfig


# ── Module-level engine singleton ─────────────────────────────────────────────
_engine: sqlalchemy.engine.Engine | None = None


def get_engine() -> sqlalchemy.engine.Engine:
    """
    Return (or create) the SQLAlchemy engine.
    Reuses the same engine across the entire process — do not call create_engine
    anywhere else in the project.
    """
    global _engine
    if _engine is None:
        url = DBConfig.sqlalchemy_url()
        _engine = create_engine(
            url,
            pool_pre_ping=True,       # verify connection before use
            pool_size=5,
            max_overflow=10,
            echo=False,               # set True to log all SQL (very verbose)
        )
        logger.info(
            f"SQLAlchemy engine created → {DBConfig.SERVER}/{DBConfig.NAME}"
        )
    return _engine


def get_connection() -> pyodbc.Connection:
    """
    Return a raw pyodbc connection.
    Caller is responsible for closing it (use as context manager).

    Usage:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(...)
    """
    conn_str = DBConfig.connection_string()
    conn = pyodbc.connect(conn_str, autocommit=False)
    return conn


def test_connection() -> bool:
    """
    Quick connectivity check — returns True if the DB is reachable.
    Run this at ETL startup to fail fast with a clear error.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() AS db, GETDATE() AS ts"))
            row = result.fetchone()
            logger.success(f"✔ Connected to: {row.db}  |  Server time: {row.ts}")
        return True
    except Exception as e:
        logger.error(f"✘ Database connection failed: {e}")
        return False

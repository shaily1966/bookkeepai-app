"""
BookKeep AI Pro — Database Connection Layer v1.0
=================================================
Single source of truth for database connections.

- Production (Railway): uses DATABASE_URL env var → PostgreSQL
- Local development:    falls back to SQLite (vendor_memory.db)

All other modules import get_conn() from here.
Never import sqlite3 or psycopg2 directly in other modules.

Usage:
    from db import get_conn, is_postgres, upsert_sql, now_sql

    conn = get_conn()
    conn.execute("SELECT 1")
    conn.commit()
    conn.close()
"""

import os
import logging

logger = logging.getLogger("bookkeep_ai.db")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SQLITE_PATH  = os.environ.get("SQLITE_PATH", "vendor_memory.db")


def is_postgres():
    """True when running against PostgreSQL (Railway production)."""
    return bool(DATABASE_URL)


def get_conn():
    """
    Return a database connection.
    Postgres in production, SQLite locally.
    Caller is responsible for conn.close().
    """
    if is_postgres():
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            return conn
        except Exception as e:
            logger.error(f"Postgres connection failed: {e}")
            raise
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn


def placeholder(n=1):
    """
    Return the correct parameter placeholder for the active DB.
    Postgres uses %s, SQLite uses ?.

    Usage:
        p = placeholder()
        conn.execute(f"SELECT * FROM t WHERE id = {p}", (id_val,))

    For multiple params:
        p = placeholder()
        conn.execute(f"INSERT INTO t (a,b) VALUES ({p},{p})", (a, b))
    """
    return "%s" if is_postgres() else "?"


def placeholders(n):
    """
    Return n placeholders as a comma-separated string.
    e.g. placeholders(3) → "%s,%s,%s" or "?,?,?"
    """
    p = placeholder()
    return ",".join([p] * n)


def upsert_sql(table, conflict_col, insert_cols, update_cols):
    """
    Generate an UPSERT statement compatible with both Postgres and SQLite.

    Postgres: INSERT ... ON CONFLICT (col) DO UPDATE SET ...
    SQLite:   INSERT INTO ... ON CONFLICT(col) DO UPDATE SET ...

    Args:
        table:        table name
        conflict_col: the UNIQUE column to conflict on (string)
        insert_cols:  list of column names to insert
        update_cols:  list of column names to update on conflict

    Returns SQL string with correct placeholders for active DB.

    Example:
        sql = upsert_sql(
            "vendor_memory",
            "key",
            ["key", "category", "updated"],
            ["category", "updated"]
        )
        conn.execute(sql, (key, category, ts))
    """
    p = placeholder()
    col_list    = ", ".join(insert_cols)
    val_list    = ", ".join([p] * len(insert_cols))
    update_list = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)

    return (
        f"INSERT INTO {table} ({col_list}) VALUES ({val_list}) "
        f"ON CONFLICT({conflict_col}) DO UPDATE SET {update_list}"
    )


def now_sql():
    """
    Return current timestamp as string (ISO format).
    Same format for both Postgres and SQLite.
    """
    from datetime import datetime
    return datetime.now().isoformat()


def create_all_tables():
    """
    Create all BookKeep AI tables if they don't exist.
    Safe to call on every startup — uses CREATE TABLE IF NOT EXISTS.
    Call this once at app startup.
    """
    conn = get_conn()
    try:
        if is_postgres():
            _create_postgres(conn)
        else:
            _create_sqlite(conn)
        conn.commit()
        logger.info(f"DB tables verified ({'PostgreSQL' if is_postgres() else 'SQLite'})")
    except Exception as e:
        logger.error(f"create_all_tables failed: {e}")
        raise
    finally:
        conn.close()


def _create_sqlite(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS vendor_memory (
            key     TEXT PRIMARY KEY,
            category TEXT,
            updated  TEXT
        );

        CREATE TABLE IF NOT EXISTS clients (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT UNIQUE,
            industry       TEXT,
            province       TEXT,
            structure      TEXT,
            account_type   TEXT,
            updated        TEXT
        );

        CREATE TABLE IF NOT EXISTS vendor_rules (
            keyword    TEXT PRIMARY KEY,
            category   TEXT,
            match_type TEXT,
            updated    TEXT
        );

        CREATE TABLE IF NOT EXISTS statements (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name           TEXT NOT NULL,
            period                TEXT,
            file_names            TEXT,
            bank                  TEXT,
            province              TEXT,
            industry              TEXT,
            business_structure    TEXT,
            processed_date        TEXT,
            total_transactions    INTEGER DEFAULT 0,
            total_expenses        REAL    DEFAULT 0,
            total_income          REAL    DEFAULT 0,
            total_payments        REAL    DEFAULT 0,
            total_itc             REAL    DEFAULT 0,
            uncategorized_count   INTEGER DEFAULT 0,
            cca_asset_count       INTEGER DEFAULT 0,
            t5018_count           INTEGER DEFAULT 0,
            extraction_confidence INTEGER DEFAULT 0,
            api_cost              REAL    DEFAULT 0,
            statement_notes       TEXT
        );

        CREATE TABLE IF NOT EXISTS client_transactions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id INTEGER NOT NULL
                         REFERENCES statements(id) ON DELETE CASCADE,
            client_name  TEXT NOT NULL,
            date         TEXT,
            source       TEXT,
            description  TEXT,
            debit        REAL DEFAULT 0,
            credit       REAL DEFAULT 0,
            txn_type     TEXT,
            category     TEXT,
            t2125        TEXT,
            itc_amount   REAL DEFAULT 0,
            confidence   TEXT,
            notes        TEXT
        );

        CREATE TABLE IF NOT EXISTS category_totals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id INTEGER NOT NULL
                         REFERENCES statements(id) ON DELETE CASCADE,
            client_name  TEXT NOT NULL,
            period       TEXT,
            category     TEXT,
            total_amount REAL DEFAULT 0,
            total_itc    REAL DEFAULT 0,
            txn_count    INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_stmts_client
            ON statements(client_name, processed_date);
        CREATE INDEX IF NOT EXISTS idx_txns_statement
            ON client_transactions(statement_id);
        CREATE INDEX IF NOT EXISTS idx_txns_client
            ON client_transactions(client_name, date);
        CREATE INDEX IF NOT EXISTS idx_cats_client
            ON category_totals(client_name, period);
    """)


def _create_postgres(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendor_memory (
            key      TEXT PRIMARY KEY,
            category TEXT,
            updated  TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id           SERIAL PRIMARY KEY,
            name         TEXT UNIQUE,
            industry     TEXT,
            province     TEXT,
            structure    TEXT,
            account_type TEXT,
            updated      TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendor_rules (
            keyword    TEXT PRIMARY KEY,
            category   TEXT,
            match_type TEXT,
            updated    TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS statements (
            id                    SERIAL PRIMARY KEY,
            client_name           TEXT NOT NULL,
            period                TEXT,
            file_names            TEXT,
            bank                  TEXT,
            province              TEXT,
            industry              TEXT,
            business_structure    TEXT,
            processed_date        TEXT,
            total_transactions    INTEGER DEFAULT 0,
            total_expenses        NUMERIC DEFAULT 0,
            total_income          NUMERIC DEFAULT 0,
            total_payments        NUMERIC DEFAULT 0,
            total_itc             NUMERIC DEFAULT 0,
            uncategorized_count   INTEGER DEFAULT 0,
            cca_asset_count       INTEGER DEFAULT 0,
            t5018_count           INTEGER DEFAULT 0,
            extraction_confidence INTEGER DEFAULT 0,
            api_cost              NUMERIC DEFAULT 0,
            statement_notes       TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS client_transactions (
            id           SERIAL PRIMARY KEY,
            statement_id INTEGER NOT NULL
                         REFERENCES statements(id) ON DELETE CASCADE,
            client_name  TEXT NOT NULL,
            date         TEXT,
            source       TEXT,
            description  TEXT,
            debit        NUMERIC DEFAULT 0,
            credit       NUMERIC DEFAULT 0,
            txn_type     TEXT,
            category     TEXT,
            t2125        TEXT,
            itc_amount   NUMERIC DEFAULT 0,
            confidence   TEXT,
            notes        TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS category_totals (
            id           SERIAL PRIMARY KEY,
            statement_id INTEGER NOT NULL
                         REFERENCES statements(id) ON DELETE CASCADE,
            client_name  TEXT NOT NULL,
            period       TEXT,
            category     TEXT,
            total_amount NUMERIC DEFAULT 0,
            total_itc    NUMERIC DEFAULT 0,
            txn_count    INTEGER DEFAULT 0
        )
    """)
    # Indexes
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_stmts_client ON statements(client_name, processed_date)",
        "CREATE INDEX IF NOT EXISTS idx_txns_statement ON client_transactions(statement_id)",
        "CREATE INDEX IF NOT EXISTS idx_txns_client ON client_transactions(client_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_cats_client ON category_totals(client_name, period)",
    ]:
        cur.execute(idx_sql)
    cur.close()

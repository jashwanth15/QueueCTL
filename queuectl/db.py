"""
db.py — Database connection and schema setup.

Every module that needs a DB connection should call get_connection().
WAL mode is enabled once on first connection so that concurrent readers
and writers (separate OS processes) don't block each other.
"""
import sqlite3
import os

# The DB file lives next to the queuectl/ package directory.
# __file__ is queuectl/db.py, so two .dirname() calls gets us to the project root.
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queue.db")


def get_connection():
    """
    Open and return a SQLite connection with:
    - WAL journal mode  → readers don't block writers and vice-versa
    - 30-second busy timeout → if the DB is locked, wait up to 30s before raising
    - Row factory      → rows accessible as dicts (row['column'])
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row  # lets us do row['state'] instead of row[2]
    # WAL mode allows concurrent readers alongside one writer.
    # It is safe to set this every connection; SQLite is idempotent about it.
    conn.execute("PRAGMA journal_mode=WAL;")
    # foreign_keys not needed here but good practice
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db():
    """
    Create the tables if they don't already exist.
    Calling this multiple times is safe (IF NOT EXISTS).
    """
    conn = get_connection()
    with conn:  # 'with conn' is a transaction — commits on success, rolls back on exception
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id                TEXT PRIMARY KEY,
                command           TEXT NOT NULL,
                state             TEXT NOT NULL,
                attempts          INTEGER NOT NULL DEFAULT 0,
                max_retries       INTEGER NOT NULL DEFAULT 3,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                next_attempt_at   TEXT,
                lease_expires_at  TEXT,
                worker_id         TEXT
            );

            CREATE TABLE IF NOT EXISTS workers (
                pid               INTEGER PRIMARY KEY,
                started_at        TEXT NOT NULL,
                last_heartbeat    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
    conn.close()
    return conn

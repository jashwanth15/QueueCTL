"""
models.py — Core database query functions.

This module contains all the functions that read/write job state.
Keeping them here (separate from worker logic) makes it easy to
understand what each operation does and to test them independently.
"""
import sqlite3
import time
from datetime import datetime, timezone


def now_iso():
    """Return the current UTC time as an ISO 8601 string (e.g. 2025-11-04T10:30:00Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ts():
    """Return current Unix timestamp as a float (for lease comparisons)."""
    return time.time()


def get_config(conn, key, default=None):
    """Read a value from the config table. Returns default if key not found."""
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_config(conn, key, value):
    """Insert or update a config value."""
    with conn:
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


def insert_job(conn, job_id, command, max_retries):
    """
    Insert a new job into the jobs table with state='pending'.
    max_retries is copied from current config at enqueue time — it won't
    change if config is updated later (each job owns its own retry budget).
    """
    ts = now_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries,
                              created_at, updated_at, next_attempt_at)
            VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
            """,
            (job_id, command, max_retries, ts, ts, ts),
        )


def reap_expired_leases(conn):
    """
    CRASH RECOVERY STEP — called at the top of every worker loop.

    Any job that is still in 'processing' but whose lease_expires_at has
    passed is assumed to belong to a dead worker (crashed via SIGKILL or
    otherwise). We reset it to 'pending' so it can be claimed again.

    This needs no lock beyond a normal write transaction because:
    - The UPDATE is atomic in SQLite.
    - Even if two workers both run this simultaneously, the second UPDATE
      hits 0 rows (already fixed by the first) and is harmless.
    """
    ts = now_iso()
    with conn:
        conn.execute(
            """
            UPDATE jobs
            SET state = 'pending',
                updated_at = ?,
                lease_expires_at = NULL,
                worker_id = NULL
            WHERE state = 'processing'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < ?
            """,
            (ts, ts),
        )


def claim_job(conn, worker_id, lease_seconds=20):
    """
    Atomically claim one eligible job for this worker.

    ── WHY THIS IS SAFE ACROSS SEPARATE OS PROCESSES ──
    We use 'BEGIN IMMEDIATE' which asks SQLite to acquire the write lock
    (a file-level lock on queue.db) *immediately*, before we even read.
    Only one process can hold this lock at a time — all other processes
    calling BEGIN IMMEDIATE will block (up to timeout=30s set in db.py).
    This makes the entire SELECT + UPDATE below one indivisible operation
    across any number of OS processes sharing the same DB file.
    Without BEGIN IMMEDIATE, two workers could both SELECT the same row,
    then both UPDATE it, running the job twice. With it, that's impossible.
    ────────────────────────────────────────────────────────────────────
    """
    now = now_iso()
    # Calculate lease expiry: current time + lease_seconds
    from datetime import datetime, timedelta, timezone
    expires_dt = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
    expires_at = expires_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # BEGIN IMMEDIATE acquires the write lock right now.
        conn.execute("BEGIN IMMEDIATE")

        # Find the oldest eligible job:
        #   - 'pending': never been attempted
        #   - 'failed' with next_attempt_at <= now: backoff delay has elapsed
        row = conn.execute(
            """
            SELECT id FROM jobs
            WHERE (state = 'pending')
               OR (state = 'failed' AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?)
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (now,),
        ).fetchone()

        if row is None:
            # No eligible job found; release the lock.
            conn.execute("ROLLBACK")
            return None

        job_id = row["id"]

        # Claim the job: mark it as processing, record which worker owns it,
        # and set the lease expiry time.
        # The WHERE clause re-checks the state — in case another worker
        # somehow got between our SELECT and UPDATE (shouldn't happen with
        # BEGIN IMMEDIATE, but defence-in-depth).
        conn.execute(
            """
            UPDATE jobs
            SET state = 'processing',
                worker_id = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state IN ('pending', 'failed')
            """,
            (worker_id, expires_at, now, job_id),
        )

        conn.execute("COMMIT")

        # Fetch the full job row to return to the caller.
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    except Exception:
        # If anything goes wrong, roll back so the job stays in its original state.
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


def complete_job(conn, job_id):
    """Mark a job as successfully completed."""
    with conn:
        conn.execute(
            "UPDATE jobs SET state='completed', updated_at=?, lease_expires_at=NULL, worker_id=NULL WHERE id=?",
            (now_iso(), job_id),
        )


def fail_job(conn, job_id, backoff_base=2):
    """
    Mark a job as failed after one unsuccessful attempt.

    - Increment attempts.
    - If attempts >= max_retries: move to 'dead' (DLQ).
    - Otherwise: move to 'failed' and compute next_attempt_at using
      exponential backoff: delay = backoff_base ** attempts (seconds).
    """
    now = now_iso()
    row = conn.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return

    new_attempts = row["attempts"] + 1

    if new_attempts >= row["max_retries"]:
        # Exhausted retries → Dead Letter Queue
        with conn:
            conn.execute(
                """
                UPDATE jobs
                SET state='dead', attempts=?, updated_at=?,
                    lease_expires_at=NULL, worker_id=NULL
                WHERE id=?
                """,
                (new_attempts, now, job_id),
            )
    else:
        # Still have retries left — compute backoff delay
        delay = backoff_base ** new_attempts  # e.g. 2^1=2s, 2^2=4s, 2^3=8s
        from datetime import datetime, timedelta, timezone
        next_attempt_dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
        next_attempt_at = next_attempt_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        with conn:
            conn.execute(
                """
                UPDATE jobs
                SET state='failed', attempts=?, updated_at=?,
                    next_attempt_at=?, lease_expires_at=NULL, worker_id=NULL
                WHERE id=?
                """,
                (new_attempts, now, next_attempt_at, job_id),
            )


def get_job_counts(conn):
    """Return a dict of {state: count} for all jobs."""
    rows = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state"
    ).fetchall()
    return {row["state"]: row["cnt"] for row in rows}


def list_jobs(conn, state):
    """Return a list of all jobs with the given state, as Row objects."""
    return conn.execute(
        "SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC", (state,)
    ).fetchall()


def get_all_workers(conn):
    """Return all rows from the workers table."""
    return conn.execute("SELECT * FROM workers ORDER BY started_at ASC").fetchall()


def register_worker(conn, pid):
    """Add this worker's PID to the workers table so 'worker stop' can find it."""
    ts = now_iso()
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO workers (pid, started_at, last_heartbeat, stop_requested) VALUES (?, ?, ?, 0)",
            (pid, ts, ts),
        )


def heartbeat_worker(conn, pid):
    """Update the last_heartbeat for this worker so we know it's still alive."""
    with conn:
        conn.execute(
            "UPDATE workers SET last_heartbeat=? WHERE pid=?",
            (now_iso(), pid),
        )


def unregister_worker(conn, pid):
    """Remove this worker's PID from the workers table on clean exit."""
    with conn:
        conn.execute("DELETE FROM workers WHERE pid=?", (pid,))


def dlq_list(conn):
    """Return all jobs in the 'dead' state."""
    return list_jobs(conn, "dead")


def dlq_retry(conn, job_id):
    """
    Re-enqueue a dead job.

    Resets attempts to 0 — the human who retried it is signaling the
    underlying issue is likely fixed, so the job deserves a full fresh
    retry budget rather than being one failure away from dying again.
    """
    ts = now_iso()
    with conn:
        rowcount = conn.execute(
            """
            UPDATE jobs
            SET state='pending', attempts=0, updated_at=?, next_attempt_at=?,
                lease_expires_at=NULL, worker_id=NULL
            WHERE id=? AND state='dead'
            """,
            (ts, ts, job_id),
        ).rowcount
    return rowcount > 0  # True if a row was actually updated

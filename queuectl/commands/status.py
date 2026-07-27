"""
commands/status.py — Handle the 'queuectl status' command.

Prints a human-readable summary of all job states and active workers.
"""
import sys
from queuectl.db import get_connection, init_db
from queuectl.models import get_job_counts, get_all_workers


def run(args):
    init_db()
    conn = get_connection()

    # --- Job counts by state ---
    counts = get_job_counts(conn)
    states = ["pending", "processing", "completed", "failed", "dead"]

    print("=== Job Status ===")
    for state in states:
        count = counts.get(state, 0)
        print(f"  {state:<12} {count}")

    total = sum(counts.values())
    print(f"  {'TOTAL':<12} {total}")

    # --- Active workers ---
    workers = get_all_workers(conn)
    print("\n=== Active Workers ===")
    if workers:
        for w in workers:
            print(f"  PID {w['pid']:>8}  started={w['started_at']}  heartbeat={w['last_heartbeat']}")
    else:
        print("  (none)")

    conn.close()

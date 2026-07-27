"""
commands/dlq.py — Handle 'queuectl dlq list' and 'queuectl dlq retry'.
"""
import json
import sys
from queuectl.db import get_connection, init_db
from queuectl.models import dlq_list, dlq_retry


def run_list(args):
    """List all jobs in the Dead Letter Queue."""
    init_db()
    conn = get_connection()
    jobs = [dict(row) for row in dlq_list(conn)]
    conn.close()

    if not jobs:
        print("DLQ is empty.")
        return
    print(f"Dead Letter Queue ({len(jobs)} job(s)):")
    for j in jobs:
        print(f"  [{j['id']}] attempts={j['attempts']}  cmd={j['command']!r}  updated={j['updated_at']}")


def run_retry(args):
    """Re-enqueue a dead job by ID, resetting its attempt count to 0."""
    init_db()
    conn = get_connection()
    success = dlq_retry(conn, args.job_id)
    conn.close()

    if success:
        print(f"Job '{args.job_id}' re-enqueued (attempts reset to 0).")
    else:
        print(f"Error: Job '{args.job_id}' not found in DLQ.", file=sys.stderr)
        sys.exit(1)

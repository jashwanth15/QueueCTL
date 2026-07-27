"""
commands/list_cmd.py — Handle the 'queuectl list' command.

IMPORTANT: When --json is used, ONLY the JSON array goes to stdout.
All other output (logging, errors) must go to stderr to avoid
breaking automated parsers.
"""
import json
import sys
from queuectl.db import get_connection, init_db
from queuectl.models import list_jobs


def run(args):
    init_db()
    conn = get_connection()

    rows = list_jobs(conn, args.state)
    conn.close()

    # Convert sqlite3.Row objects to plain dicts so json.dumps can serialize them.
    jobs = [dict(row) for row in rows]

    if args.json:
        # Contract: ONLY the JSON array on stdout, nothing else.
        print(json.dumps(jobs, indent=2))
    else:
        # Human-readable table
        if not jobs:
            print(f"No jobs in state '{args.state}'.")
            return
        print(f"Jobs in state '{args.state}':")
        for j in jobs:
            print(f"  [{j['id']}] cmd={j['command']!r}  attempts={j['attempts']}/{j['max_retries']}  updated={j['updated_at']}")

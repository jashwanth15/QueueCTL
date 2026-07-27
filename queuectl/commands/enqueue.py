"""
commands/enqueue.py — Handle the 'queuectl enqueue' command.
"""
import json
import sys
from queuectl.db import get_connection, init_db
from queuectl.models import insert_job, get_config


def run(args):
    """
    Parse the JSON job spec from args.job_json, validate required fields,
    and insert the job into the database.
    """
    try:
        job = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON — {e}", file=sys.stderr)
        sys.exit(1)

    # Validate required fields
    if "id" not in job or "command" not in job:
        print("Error: Job JSON must include 'id' and 'command' fields.", file=sys.stderr)
        sys.exit(1)

    init_db()
    conn = get_connection()

    # Read max_retries from config (default 3); the job stores this at creation time.
    max_retries = int(get_config(conn, "max-retries", default="3"))

    # Allow overriding max_retries per-job in the JSON spec.
    if "max_retries" in job:
        max_retries = int(job["max_retries"])

    try:
        insert_job(conn, job["id"], job["command"], max_retries)
        print(f"Enqueued job '{job['id']}'")
    except Exception as e:
        # Most likely a UNIQUE constraint failure (duplicate ID).
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

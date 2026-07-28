"""
demo_setup.py — Enqueue a fresh set of demo jobs.

Safe to run while workers are already running.
Clears old demo jobs by ID first, then re-inserts them.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job

init_db()
conn = get_connection()

DEMO_JOB_IDS = ["job-1", "job-2", "job-3", "job-4", "job-5", "job-6"]

# Remove any old demo jobs by ID so re-running is safe
with conn:
    for jid in DEMO_JOB_IDS:
        conn.execute("DELETE FROM jobs WHERE id=?", (jid,))

jobs = [
    ("job-1", "echo Hello from job 1", 3),
    ("job-2", "echo Hello from job 2", 3),
    ("job-3", "echo Hello from job 3", 3),
    ("job-4", "python -c \"import time; time.sleep(3); print('Job 4 done after 3s sleep')\"", 3),
    ("job-5", "exit 1", 2),   # always fails -> hits DLQ after 2 attempts
    ("job-6", "echo Final job", 3),
]

for jid, cmd, mr in jobs:
    insert_job(conn, jid, cmd, mr)
    print(f"Enqueued {jid}: {cmd[:55]}")

conn.close()
print("\nAll jobs enqueued!")
print("Now start workers in this terminal:")
print("  python queuectl.py worker start --count 2")

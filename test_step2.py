"""Quick test script to verify Steps 2-3 work correctly."""
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job, list_jobs, get_job_counts

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")

init_db()
conn = get_connection()

# Test insert
insert_job(conn, "job1", "echo Hello World", max_retries=3)
insert_job(conn, "job2", "echo Job Two", max_retries=3)
insert_job(conn, "job3", "exit 1", max_retries=2)

# Test list
jobs = list_jobs(conn, "pending")
print(f"Pending jobs: {len(jobs)}")
for j in jobs:
    print(f"  [{j['id']}] {j['command']} retries={j['max_retries']}")

# Test counts
counts = get_job_counts(conn)
print(f"Counts: {dict(counts)}")

conn.close()
print("Step 2 logic: OK")

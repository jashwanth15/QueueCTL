"""
Test Step 5: Atomic cross-process claiming.

Enqueues 20 jobs. Each job appends its ID to a side-effect log file.
Starts 3 workers simultaneously. Waits for all jobs to complete.
Verifies:
  1. Every job ran EXACTLY ONCE (no duplicate log entries).
  2. Every job reached state=completed.
"""
import sys
import os
import time
import subprocess
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

LOG_FILE = "step5_side_effect.log"

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job, get_job_counts

init_db()
conn = get_connection()

# Each job appends its own ID to the log file.
# Using '>>' so multiple jobs don't clobber each other.
NUM_JOBS = 20
for i in range(NUM_JOBS):
    job_id = f"job-{i:03d}"
    cmd = f"echo {job_id} >> {LOG_FILE}"
    insert_job(conn, job_id, cmd, max_retries=3)

print(f"Enqueued {NUM_JOBS} jobs")

# Start 3 worker processes
workers = []
for _ in range(3):
    p = subprocess.Popen(
        [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
        stderr=subprocess.DEVNULL,
    )
    workers.append(p)

print(f"Started 3 workers: PIDs={[p.pid for p in workers]}")

# Wait up to 60 seconds for all jobs to complete
deadline = time.time() + 60
all_done = False
while time.time() < deadline:
    time.sleep(1)
    counts = get_job_counts(conn)
    completed = counts.get("completed", 0)
    print(f"  completed={completed}/{NUM_JOBS}")
    if completed == NUM_JOBS:
        all_done = True
        break

# Stop all workers
for p in workers:
    p.terminate()
for p in workers:
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()

print()
if not all_done:
    counts = get_job_counts(conn)
    print(f"FAIL: Not all jobs completed. Counts: {dict(counts)}")
    sys.exit(1)

# Verify the log file: each job should appear exactly once
if not os.path.exists(LOG_FILE):
    print(f"FAIL: Log file '{LOG_FILE}' not found — no jobs ran?")
    sys.exit(1)

with open(LOG_FILE) as f:
    lines = [l.strip() for l in f.readlines() if l.strip()]

print(f"Log file has {len(lines)} entries (expected {NUM_JOBS})")

from collections import Counter
counts_log = Counter(lines)
duplicates = {job_id: cnt for job_id, cnt in counts_log.items() if cnt > 1}
missing = [f"job-{i:03d}" for i in range(NUM_JOBS) if f"job-{i:03d}" not in counts_log]

if duplicates:
    print(f"FAIL: Duplicate job executions detected: {duplicates}")
    sys.exit(1)
if missing:
    print(f"FAIL: Some jobs never ran: {missing}")
    sys.exit(1)

print("All 20 jobs ran exactly once. No duplicates. No missing jobs.")
print("Step 5: OK - atomic cross-process claiming verified")
conn.close()

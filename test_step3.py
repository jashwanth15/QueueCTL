"""
Test Step 3: Single worker happy path (subprocess-based).

Starts a single worker as a separate OS process, waits for the job
to complete, then stops the worker.
"""
import sys
import os
import time
import subprocess
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job

init_db()
conn = get_connection()

# Enqueue a simple successful job
insert_job(conn, "happy-job", "echo step3-happy-path", max_retries=3)
print("Enqueued happy-job (echo command)")

# Start a worker subprocess in the background
worker_proc = subprocess.Popen(
    [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
    stderr=subprocess.PIPE,  # capture worker's log output
)
print(f"Started worker subprocess PID={worker_proc.pid}")

# Poll the DB for up to 10 seconds to see if the job completes
deadline = time.time() + 10
completed = False
while time.time() < deadline:
    time.sleep(0.5)
    # Re-read from DB each iteration (open fresh connection)
    row = conn.execute("SELECT state FROM jobs WHERE id='happy-job'").fetchone()
    if row and row["state"] == "completed":
        print("happy-job -> state=completed [OK]")
        completed = True
        break

# Gracefully stop the worker
worker_proc.terminate()
try:
    worker_proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    worker_proc.kill()

# Print worker stderr so we can see what it did
stderr_out = worker_proc.stderr.read().decode(errors="replace")
print("--- Worker output ---")
print(stderr_out)
print("---------------------")

if not completed:
    row = conn.execute("SELECT state FROM jobs WHERE id='happy-job'").fetchone()
    print(f"FAIL: job still in state '{row['state'] if row else 'missing'}'")
    sys.exit(1)

conn.close()
print("Step 3: OK")

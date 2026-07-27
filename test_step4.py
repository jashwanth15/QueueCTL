"""
Test Step 4: Retry + exponential backoff + DLQ transition.

Enqueues a job with command 'exit 1' (always fails) and max_retries=2.
Expected behaviour:
  attempt 0 -> fails -> state=failed, next_attempt_at = now + 2^1 = 2s
  attempt 1 -> fails -> state=failed, next_attempt_at = now + 2^2 = 4s
  attempt 2 -> fails -> attempts >= max_retries -> state=dead
"""
import sys
import os
import time
import subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job

init_db()
conn = get_connection()

# Job that always fails, max_retries=2 means 2 allowed attempts
insert_job(conn, "fail-job", "exit 1", max_retries=2)
print("Enqueued fail-job (exit 1, max_retries=2)")

# Start a worker subprocess
worker_proc = subprocess.Popen(
    [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
    stderr=subprocess.PIPE,
)
print(f"Worker PID={worker_proc.pid}")

# Poll for up to 30 seconds for the job to reach 'dead'
deadline = time.time() + 30
dead = False
while time.time() < deadline:
    time.sleep(1)
    row = conn.execute("SELECT state, attempts FROM jobs WHERE id='fail-job'").fetchone()
    state = row["state"] if row else "missing"
    attempts = row["attempts"] if row else -1
    print(f"  state={state} attempts={attempts}")
    if state == "dead":
        dead = True
        break

# Stop worker
worker_proc.terminate()
try:
    worker_proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    worker_proc.kill()

stderr_out = worker_proc.stderr.read().decode(errors="replace")
print("--- Worker output ---")
print(stderr_out)
print("---------------------")

if not dead:
    print("FAIL: Job never reached 'dead' state")
    sys.exit(1)

row = conn.execute("SELECT * FROM jobs WHERE id='fail-job'").fetchone()
print(f"Final state: state={row['state']} attempts={row['attempts']} max_retries={row['max_retries']}")
assert row["state"] == "dead", f"Expected 'dead', got '{row['state']}'"
assert row["attempts"] == 2, f"Expected 2 attempts, got {row['attempts']}"
print("Step 4: OK - job retried and moved to DLQ as expected")
conn.close()

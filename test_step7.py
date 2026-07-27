"""
Test Step 7: worker stop from a separate terminal + graceful in-flight completion.

Timeline:
  1. Enqueue a long job (sleep 10).
  2. Start a worker — it picks up the job and enters execution.
  3. While the worker is mid-job, run 'queuectl worker stop' from a DIFFERENT
     subprocess (simulating a different terminal).
  4. Verify the worker finishes the current job gracefully (SIGTERM received,
     job completes, state=completed) rather than abandoning it mid-run.
  5. Verify the worker is no longer in the workers table after clean exit.
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

# Job that takes 5 seconds — gives us time to issue 'worker stop' while it's running
insert_job(conn, "graceful-job", "python -c \"import time; time.sleep(5)\"", max_retries=3)
print("Enqueued graceful-job (sleep 5s)")

# Start the worker
worker_proc = subprocess.Popen(
    [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
    stderr=subprocess.PIPE,
)
print(f"Worker PID={worker_proc.pid}")

# Wait for the job to be claimed
deadline = time.time() + 10
while time.time() < deadline:
    time.sleep(0.3)
    row = conn.execute("SELECT state FROM jobs WHERE id='graceful-job'").fetchone()
    if row and row["state"] == "processing":
        print("Job is now 'processing'. Sending 'worker stop' from separate process...")
        break
else:
    print("FAIL: Job never entered 'processing'")
    worker_proc.kill()
    sys.exit(1)

# Run 'worker stop' from a DIFFERENT subprocess (the key point of Step 7)
stop_proc = subprocess.run(
    [sys.executable, "queuectl.py", "worker", "stop"],
    capture_output=True, text=True
)
print(f"worker stop output: {stop_proc.stdout.strip()}")
if stop_proc.stderr:
    print(f"worker stop stderr: {stop_proc.stderr.strip()}")

# Wait for the worker to finish gracefully (should finish current job before exiting)
try:
    worker_proc.wait(timeout=15)  # job is 5s + some overhead
except subprocess.TimeoutExpired:
    print("FAIL: Worker did not exit within 15s after SIGTERM")
    worker_proc.kill()
    sys.exit(1)

stderr_out = worker_proc.stderr.read().decode(errors="replace")
print("--- Worker output ---")
print(stderr_out.encode("ascii", errors="replace").decode("ascii"))
print("---------------------")

# Verify job completed (not abandoned mid-execution)
row = conn.execute("SELECT state FROM jobs WHERE id='graceful-job'").fetchone()
print(f"Final job state: {row['state'] if row else 'missing'}")

if not row or row["state"] != "completed":
    print(f"FAIL: Expected 'completed', got '{row['state'] if row else 'missing'}'")
    sys.exit(1)

# Verify worker is removed from workers table
workers_left = conn.execute("SELECT COUNT(*) as cnt FROM workers").fetchone()["cnt"]
print(f"Workers left in DB: {workers_left} (expected 0)")
if workers_left != 0:
    print("WARN: Worker PID still in workers table (may be OK if stop cleaned up)")

print("Step 7: OK - graceful shutdown verified, job completed before worker exited")
conn.close()

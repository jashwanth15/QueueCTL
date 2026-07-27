"""
Test Step 6: Crash recovery via lease expiry.

Timeline:
  1. Enqueue a long-running job (sleep 30) with a short lease (20s default).
  2. Start a worker, wait until it claims the job (state=processing).
  3. SIGKILL the worker (simulating a crash — no cleanup runs).
  4. Start a SECOND worker and wait for the lease to expire and the job to be reaped.
  5. The second worker should detect the expired lease and reset the job to pending,
     then claim and complete it.
  6. Verify total recovery time is under 60 seconds.

Note: 'sleep 30' on Windows uses a Python-based sleep to stay cross-platform.
"""
import sys
import os
import time
import subprocess
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job

init_db()
conn = get_connection()

# Use a Python one-liner for sleep so it works on Windows too
# This job just sleeps 30 seconds — long enough for us to SIGKILL the worker
insert_job(conn, "long-job", "python -c \"import time; time.sleep(30)\"", max_retries=3)
print("Enqueued long-job (sleeps 30s)")

# Start a worker
worker_proc = subprocess.Popen(
    [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
    stderr=subprocess.PIPE,
)
print(f"Worker 1 PID={worker_proc.pid}")

# Wait for the job to be claimed (state=processing)
deadline = time.time() + 10
while time.time() < deadline:
    time.sleep(0.5)
    row = conn.execute("SELECT state FROM jobs WHERE id='long-job'").fetchone()
    if row and row["state"] == "processing":
        print(f"Job is now 'processing' (lease_expires_at: {conn.execute('SELECT lease_expires_at FROM jobs WHERE id=?', ('long-job',)).fetchone()[0]})")
        break
else:
    print("FAIL: Job never entered 'processing' state")
    worker_proc.kill()
    sys.exit(1)

# SIGKILL the worker — simulating a hard crash (no SIGTERM handler runs)
kill_time = time.time()
print(f"Sending SIGKILL to worker PID={worker_proc.pid}...")
worker_proc.kill()
worker_proc.wait()
print("Worker killed.")

# Start a second worker — it will run reap_expired_leases() on every loop tick
worker2_proc = subprocess.Popen(
    [sys.executable, "queuectl.py", "worker", "start", "--count", "1"],
    stderr=subprocess.PIPE,
)
print(f"Worker 2 PID={worker2_proc.pid} - waiting for recovery...")

# Wait up to 60 seconds from the kill time for the job to complete
deadline = kill_time + 60
recovered = False
while time.time() < deadline:
    time.sleep(1)
    row = conn.execute("SELECT state FROM jobs WHERE id='long-job'").fetchone()
    state = row["state"] if row else "missing"
    elapsed = time.time() - kill_time
    print(f"  elapsed={elapsed:.0f}s state={state}")
    if state == "completed":
        recovered = True
        recovery_time = elapsed
        break

# Stop second worker
worker2_proc.terminate()
try:
    worker2_proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    worker2_proc.kill()

stderr2 = worker2_proc.stderr.read().decode(errors="replace")
print("--- Worker 2 output ---")
print(stderr2)
print("------------------------")

if not recovered:
    row = conn.execute("SELECT state FROM jobs WHERE id='long-job'").fetchone()
    print(f"FAIL: Job not recovered within 60s. Final state: {row['state'] if row else 'missing'}")
    sys.exit(1)

print(f"Job recovered and completed in {recovery_time:.1f}s after SIGKILL (must be < 60s)")
assert recovery_time < 60, f"Recovery took too long: {recovery_time:.1f}s"
print("Step 6: OK - crash recovery via lease expiry verified")
conn.close()

"""
tests/run_tests.py — Cross-platform Python test runner for all 5 acceptance scenarios.

Run from the project root:
    python queuectl/tests/run_tests.py

This mirrors test_scenarios.sh for Windows environments.
"""
import sys
import os
import time
import subprocess
import json
import signal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DB_FILE = "queue.db"
PY = sys.executable
PASS = 0
FAIL = 0


def _pass(msg):
    global PASS
    print(f"  [PASS] {msg}")
    PASS += 1


def _fail(msg):
    global FAIL
    print(f"  [FAIL] {msg}")
    FAIL += 1


def clean_db():
    for f in [DB_FILE, DB_FILE + "-wal", DB_FILE + "-shm"]:
        if os.path.exists(f):
            os.remove(f)


def queuectl(*args, capture=True):
    """Run a queuectl command and return the CompletedProcess."""
    cmd = [PY, "queuectl.py"] + list(args)
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    else:
        return subprocess.run(cmd)


def start_worker(count=1):
    """Start worker subprocess(es) in the background, return the Popen object."""
    return subprocess.Popen(
        [PY, "queuectl.py", "worker", "start", "--count", str(count)],
        stderr=subprocess.DEVNULL,
    )


def count_state(state):
    """Return number of jobs in the given state."""
    result = queuectl("list", "--state", state, "--json")
    if result.returncode != 0:
        return 0
    try:
        return len(json.loads(result.stdout))
    except Exception:
        return 0


def wait_for(timeout, desc, check_fn):
    """Poll check_fn() up to timeout seconds. Return True if it passes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if check_fn():
                return True
        except Exception:
            pass
        time.sleep(1)
    print(f"  TIMEOUT waiting for: {desc}")
    return False


def stop_workers(worker_proc):
    """Gracefully stop workers and wait for them to exit."""
    queuectl("worker", "stop")
    try:
        worker_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        worker_proc.kill()


print("=" * 44)
print("QueueCTL Acceptance Tests")
print("=" * 44)
print()

# ── Scenario 1: Basic job completion ─────────────────────────────────────────
print("Scenario 1: Basic job completion")
clean_db()

queuectl("enqueue", '{"id":"s1-job","command":"echo scenario1-done"}')
w = start_worker()

if wait_for(10, "job completed", lambda: count_state("completed") >= 1):
    _pass("Job reached state=completed")
else:
    _fail("Job did not complete within 10s")

stop_workers(w)
print()

# ── Scenario 2: Failing job retries and lands in DLQ ────────────────────────
print("Scenario 2: Failing job retries with backoff, lands in DLQ")
clean_db()

queuectl("enqueue", '{"id":"s2-job","command":"exit 1","max_retries":2}')
w = start_worker()

if wait_for(30, "job dead", lambda: count_state("dead") >= 1):
    _pass("Job reached state=dead after max_retries exhausted")
    result = queuectl("list", "--state", "dead", "--json")
    jobs = json.loads(result.stdout)
    attempts = jobs[0]["attempts"] if jobs else -1
    if attempts == 2:
        _pass(f"Attempts count = {attempts} (correct)")
    else:
        _fail(f"Expected attempts=2, got {attempts}")
else:
    _fail("Job did not reach dead state within 30s")

stop_workers(w)
print()

# ── Scenario 3: Many jobs, multiple workers, each runs exactly once ──────────
print("Scenario 3: 20+ jobs across 3 workers, each runs exactly once")
clean_db()

LOG_FILE = os.path.join("queuectl", "tests", "scenario3_side_effect.log")
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

# Use >> to append; on Windows the cmd is slightly different
for i in range(20):
    job_id = f"s3-job-{i:02d}"
    cmd = f"echo {job_id} >> {LOG_FILE}"
    queuectl("enqueue", json.dumps({"id": job_id, "command": cmd}))

# Start 3 separate worker processes
workers = [start_worker(1) for _ in range(3)]

if wait_for(60, "all 20 jobs completed", lambda: count_state("completed") >= 20):
    _pass("All 20 jobs completed")
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            lines = [l.strip() for l in f if l.strip()]
        total = len(lines)
        unique = len(set(lines))
        if total == 20 and unique == 20:
            _pass(f"Log has {total} unique entries — no duplicates, no missing")
        else:
            _fail(f"Log has {total} lines ({unique} unique) — expected 20 each")
    else:
        _fail("Side-effect log file not found")
else:
    completed = count_state("completed")
    _fail(f"Only {completed}/20 jobs completed within 60s")

for wp in workers:
    queuectl("worker", "stop")
    try:
        wp.wait(timeout=5)
    except subprocess.TimeoutExpired:
        wp.kill()
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
print()

# ── Scenario 4: SIGKILL mid-job, job recovers via lease expiry ───────────────
print("Scenario 4: Worker SIGKILL mid-job, job recovers within lease window")
clean_db()

# Use a sleep command that works cross-platform
sleep_cmd = f"{PY} -c \"import time; time.sleep(25)\""
queuectl("enqueue", json.dumps({"id": "s4-job", "command": sleep_cmd}))

w = start_worker()

if wait_for(10, "job processing", lambda: count_state("processing") >= 1):
    print(f"  Job is processing. SIGKILL-ing worker PID={w.pid}...")
    w.kill()
    w.wait()
    kill_time = time.time()
    print("  Worker killed. Starting recovery worker...")

    w2 = start_worker()

    if wait_for(60, "s4-job completed", lambda: count_state("completed") >= 1):
        recovery_time = time.time() - kill_time
        _pass(f"Job recovered and completed in {recovery_time:.0f}s (< 60s)")
    else:
        _fail("Job did not recover within 60s")

    stop_workers(w2)
else:
    _fail("Job never entered 'processing' state")
    w.kill()
time.sleep(2)  # Give workers a moment to fully exit before clean_db()
print()

# ── Scenario 5: Jobs survive full process restart ────────────────────────────
print("Scenario 5: Jobs survive a full process restart")
clean_db()

for i in range(1, 4):
    queuectl("enqueue", json.dumps({"id": f"s5-job{i}", "command": f"echo s5-job{i}"}))

pending = count_state("pending")
if pending == 3:
    _pass("3 jobs in pending state before restart")
else:
    _fail(f"Expected 3 pending, got {pending}")

# "Restart" = read the DB fresh from a new process
pending_after = count_state("pending")
if pending_after == 3:
    _pass("Jobs still present after restart (DB persisted)")
else:
    _fail(f"Expected 3 pending after restart, got {pending_after}")

w = start_worker()
if wait_for(30, "all 3 jobs completed", lambda: count_state("completed") >= 3):
    _pass("All 3 jobs completed after restart")
else:
    _fail("Jobs did not complete after restart")

stop_workers(w)
print()

# ── Summary ──────────────────────────────────────────────────────────────────
print("=" * 44)
print(f"Results: {PASS} passed, {FAIL} failed")
print("=" * 44)
sys.exit(0 if FAIL == 0 else 1)

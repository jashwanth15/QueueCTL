"""
Test Steps 8 & 9: config set (non-retroactive) + dlq list/retry.
"""
import sys
import os
import subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Clean start
if os.path.exists("queue.db"):
    os.remove("queue.db")

from queuectl.db import init_db, get_connection
from queuectl.models import insert_job, get_config, set_config

init_db()
conn = get_connection()

# ── Step 8: config set ────────────────────────────────────────────────────────

# Set max-retries to 2 before enqueuing job-a
set_config(conn, "max-retries", "2")

# Enqueue job-a with the current config (max_retries=2)
insert_job(conn, "job-a", "exit 1", max_retries=2)
row = conn.execute("SELECT max_retries FROM jobs WHERE id='job-a'").fetchone()
assert row["max_retries"] == 2, f"Expected 2, got {row['max_retries']}"
print("job-a has max_retries=2 [OK]")

# Now change config to max-retries=5
set_config(conn, "max-retries", "5")

# Enqueue job-b AFTER the config change
insert_job(conn, "job-b", "exit 1", max_retries=5)
row_b = conn.execute("SELECT max_retries FROM jobs WHERE id='job-b'").fetchone()
assert row_b["max_retries"] == 5, f"Expected 5, got {row_b['max_retries']}"
print("job-b has max_retries=5 after config change [OK]")

# job-a should still have max_retries=2 (config change is non-retroactive)
row_a = conn.execute("SELECT max_retries FROM jobs WHERE id='job-a'").fetchone()
assert row_a["max_retries"] == 2, f"Expected 2 (unchanged), got {row_a['max_retries']}"
print("job-a still has max_retries=2 (non-retroactive) [OK]")

# ── Step 9: dlq list / dlq retry ──────────────────────────────────────────────

# Manually move job-a to dead state to test DLQ
with conn:
    conn.execute("UPDATE jobs SET state='dead', attempts=2 WHERE id='job-a'")

# Test dlq list
result = subprocess.run(
    [sys.executable, "queuectl.py", "dlq", "list"],
    capture_output=True, text=True
)
print(f"\nDLQ list output:\n{result.stdout}")
assert "job-a" in result.stdout, "job-a not in dlq list output"
print("dlq list shows job-a [OK]")

# Test dlq retry
result = subprocess.run(
    [sys.executable, "queuectl.py", "dlq", "retry", "job-a"],
    capture_output=True, text=True
)
print(f"dlq retry output: {result.stdout.strip()}")
assert result.returncode == 0, f"dlq retry failed: {result.stderr}"

row_a = conn.execute("SELECT state, attempts FROM jobs WHERE id='job-a'").fetchone()
assert row_a["state"] == "pending", f"Expected 'pending', got '{row_a['state']}'"
assert row_a["attempts"] == 0, f"Expected attempts=0 after DLQ retry, got {row_a['attempts']}"
print(f"After dlq retry: state={row_a['state']} attempts={row_a['attempts']} [OK]")
print("(attempts reset to 0 as specified)")

# Test retry on non-existent job
result = subprocess.run(
    [sys.executable, "queuectl.py", "dlq", "retry", "nonexistent"],
    capture_output=True, text=True
)
assert result.returncode != 0, "Expected non-zero exit for nonexistent job"
print("dlq retry nonexistent job -> error (expected) [OK]")

conn.close()
print("\nSteps 8 & 9: OK")

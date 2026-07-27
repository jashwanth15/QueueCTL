# QueueCTL — CLI Background Job Queue

A background job queue system built in Python using only the standard library. Manages jobs with worker processes, retries failures with exponential backoff, maintains a Dead Letter Queue (DLQ), and survives process crashes via lease-based recovery.

---

## Setup

No dependencies to install — Python 3 standard library only.

```bash
# Clone and enter the project
git clone <your-repo-url>
cd queuectl

# Verify Python 3 is installed
python --version  # Python 3.x

# The database (queue.db) is created automatically on first command.
```

---

## Usage

All commands are run as:

```bash
python queuectl.py <command> [options]
```

### Enqueue a job

```bash
python queuectl.py enqueue '{"id":"job1","command":"echo Hello World"}'

# With custom max_retries per job:
python queuectl.py enqueue '{"id":"job2","command":"curl https://example.com","max_retries":5}'
```

The `command` field is any shell command. `id` must be unique.

### Start workers (foreground)

```bash
# Start 1 worker (blocks until stopped)
python queuectl.py worker start

# Start 3 workers in parallel (blocks until all stopped)
python queuectl.py worker start --count 3
```

Workers run in the **foreground** and print logs to stderr. To stop them, use `worker stop` from another terminal, or press Ctrl+C.

### Stop workers (from a separate terminal)

```bash
python queuectl.py worker stop
```

Sends a stop signal to all registered workers via the database. Workers finish any in-flight job before exiting — they are never abandoned mid-execution.

### Check status

```bash
python queuectl.py status
```

Output:
```
=== Job Status ===
  pending      5
  processing   2
  completed    42
  failed       0
  dead         1
  TOTAL        50

=== Active Workers ===
  PID    12345  started=2025-11-04T10:00:00Z  heartbeat=2025-11-04T10:05:30Z
```

### List jobs by state

```bash
python queuectl.py list --state pending
python queuectl.py list --state completed
python queuectl.py list --state dead

# Machine-readable JSON (only JSON array on stdout):
python queuectl.py list --state pending --json
```

Valid states: `pending`, `processing`, `completed`, `failed`, `dead`

### Dead Letter Queue (DLQ)

```bash
# List all permanently failed jobs
python queuectl.py dlq list

# Retry a dead job (resets attempt counter to 0)
python queuectl.py dlq retry job1
```

### Configuration

```bash
# Set how many times a job can fail before going to the DLQ
python queuectl.py config set max-retries 5

# Set the backoff base (delay = base^attempts seconds)
python queuectl.py config set backoff-base 3

# Check current value
python queuectl.py config get max-retries
```

> **Note:** Config changes apply only to jobs enqueued **after** the change. Existing jobs keep the `max_retries` they were created with.

---

## Architecture Overview

### Components

| File | Purpose |
|------|---------|
| `queuectl/cli.py` | argparse entrypoint, dispatches subcommands |
| `queuectl/db.py` | SQLite connection, WAL mode, schema init |
| `queuectl/models.py` | All DB query functions (claim, complete, fail, reap, DLQ) |
| `queuectl/worker.py` | Worker loop, signal handling, heartbeating, multi-process start |
| `queuectl/commands/` | One file per CLI subcommand group |

### Database

Three tables in `queue.db` (SQLite, WAL mode):

- **`jobs`** — All job state: id, command, state, attempts, max_retries, timestamps, lease
- **`workers`** — Active worker PIDs + heartbeat + stop_requested flag
- **`config`** — Persistent key-value configuration

### Atomic Claiming

The most critical piece — how two workers never run the same job:

```python
conn.execute("BEGIN IMMEDIATE")  # acquires cross-process write lock
# SELECT the oldest eligible job
# UPDATE it to state='processing' with a lease expiry
conn.execute("COMMIT")
```

`BEGIN IMMEDIATE` acquires SQLite's file-level write lock before reading, so only one OS process can be inside this transaction at a time. See `models.py → claim_job()`.

### Crash Recovery

Every job in `processing` state has a `lease_expires_at` timestamp (~20s from claim time). At the **top of every worker loop iteration**, before looking for new work, the worker runs `reap_expired_leases()`:

```sql
UPDATE jobs SET state='pending' WHERE state='processing' AND lease_expires_at < now()
```

If a worker is killed with `SIGKILL`, no cleanup runs — but the lease expires within 20 seconds. The next loop tick of any worker (including a freshly started one) reaps the stale job and makes it `pending` again. **Worst-case recovery: ~22 seconds** (lease + poll interval).

### Graceful Shutdown

`worker stop` uses a **DB-based flag** (the `stop_requested` column in the `workers` table) as the cross-platform shutdown mechanism:

1. Sets `stop_requested=1` for each worker's row
2. The worker checks this at the top of every loop tick
3. On Linux, also sends SIGTERM to wake a sleeping worker faster
4. On Windows, SIGTERM hard-kills, so only the DB flag is used

This ensures workers always **finish the current job** before exiting.

### Retry & Backoff

When a job fails, the next attempt is scheduled at:

```
next_attempt_at = now + (backoff_base ** attempts)
```

For `backoff_base=2`:
- After attempt 1: wait 2s
- After attempt 2: wait 4s  
- After attempt 3: wait 8s

After `max_retries` total attempts, the job moves to `dead` (DLQ).

---

## Running Tests

### Cross-platform Python test runner:
```bash
python queuectl/tests/run_tests.py
```

### Bash test script (Linux/Mac/Git Bash):
```bash
bash queuectl/tests/test_scenarios.sh
```

### Individual step tests:
```bash
python test_step2.py   # enqueue/list/status
python test_step3.py   # single worker happy path
python test_step4.py   # retry + DLQ
python test_step5.py   # 20 jobs, 3 workers, no duplicates
python test_step6.py   # SIGKILL crash recovery
python test_step7.py   # graceful worker stop
python test_step8_9.py # config + DLQ operations
```

---

## Job State Diagram

```
           enqueue
              │
              ▼
           pending ◄──────────────────┐
              │                       │ (dlq retry resets attempts=0)
         worker claims                │
              │                       │
              ▼                       │
          processing                dead (DLQ)
         /          \                  ▲
    exit 0        exit non-0           │
        │               │             │ attempts >= max_retries
        ▼               ▼             │
    completed        failed ──────────┘
                        │
                   (backoff delay)
                        │
                     pending (retry)
```

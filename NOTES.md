# QueueCTL — Implementation Notes

This file tracks the build walkthrough for grading purposes.

## Build Commits (git log)

1. **Step 1** — Init repo, db.py with WAL mode, schema (jobs/workers/config tables)
2. **Step 2** — enqueue, list --json, status commands working
3. **Step 3** — Single worker happy path - claim, run via subprocess, mark completed
4. **Step 4** — Retry with exponential backoff, DLQ transition after max_retries exhausted
5. **Step 5** — Atomic cross-process claiming verified - 20 jobs, 3 workers, each ran exactly once
6. **Step 6** — Crash recovery via lease expiry verified - SIGKILL mid-job, recovered in 51s (< 60s budget)
7. **Step 7** — worker stop via DB flag (cross-platform) + graceful in-flight job completion
8. **Steps 8+9** — config set (non-retroactive) and dlq list/retry (attempts reset to 0) verified
9. **Steps 10+11** — test_scenarios.sh, README.md, DECISIONS.md

## Key Design Notes for Live Review

### The 3 most important functions to know cold:

**`claim_job()` in models.py** — The heart of the system.
- `BEGIN IMMEDIATE` = cross-process write lock, not just in-memory
- SELECT + UPDATE in one transaction = no duplicate execution ever
- `lease_expires_at = now + 20s` = enables crash recovery

**`reap_expired_leases()` in models.py** — Crash recovery in 2 SQL lines.
- Runs at the TOP of every worker loop tick
- Any `processing` job with expired lease → reset to `pending`
- No daemon process needed

**`cmd_worker_stop()` in worker.py** — Cross-platform graceful shutdown.
- Sets `stop_requested=1` in DB for each worker PID
- Worker checks this at top of loop before claiming new work
- On Windows: only DB flag (SIGTERM hard-kills)
- On Linux: DB flag + SIGTERM (wakes sleeping worker faster)

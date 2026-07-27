# DECISIONS.md — QueueCTL Design Decisions

Answers to the five required questions, with specific references to the actual code built.

---

## 1. Which exact lines prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

**File:** `queuectl/models.py`, function `claim_job()`, approximately lines 75–120.

The key line is:

```python
conn.execute("BEGIN IMMEDIATE")
```

Here is why this is the complete answer:

SQLite supports three transaction modes: DEFERRED (default), IMMEDIATE, and EXCLUSIVE. `BEGIN IMMEDIATE` acquires the **write lock on the database file immediately**, before reading anything. This is a file-level lock — it is visible to **all processes** sharing the same `queue.db` file on disk, not just threads within one process.

The full sequence in `claim_job()`:

```
conn.execute("BEGIN IMMEDIATE")   # ← Acquires cross-process write lock
# SELECT oldest eligible job      # ← Safe read; no other writer can be here
# UPDATE job to state=processing  # ← Safe write
conn.execute("COMMIT")            # ← Releases lock
```

While one worker holds this lock, any other worker that calls `BEGIN IMMEDIATE` will block (waiting up to 30 seconds, as configured by `timeout=30` in `db.py → get_connection()`). Once the first worker commits and releases the lock, the second worker proceeds — but the row has already been updated to `state='processing'`, so the `WHERE state IN ('pending','failed')` guard in the UPDATE ensures no double-claim.

The defence-in-depth `WHERE` clause in the UPDATE is a secondary safety net. The primary guarantee is `BEGIN IMMEDIATE`. Without it, two workers could both read the same row in a SELECT, then both issue the UPDATE — resulting in the job running twice. `BEGIN IMMEDIATE` makes this impossible.

---

## 2. A worker is SIGKILL'd halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?

**Files:** `queuectl/models.py → claim_job()`, `reap_expired_leases()`, `queuectl/worker.py → run_worker()`

**Step-by-step:**

1. **Worker claims the job** (in `claim_job()`): sets `state='processing'`, `worker_id='0'`, `lease_expires_at=now+20s`.

2. **Worker starts executing** the job's command via `subprocess.run()`. The process is blocked here.

3. **`SIGKILL` is sent.** The OS immediately terminates the worker process. No Python code runs — `finally` blocks, signal handlers, and the `unregister_worker()` call all never execute. The job remains in `state='processing'` in the DB with a `lease_expires_at` ~20 seconds in the future.

4. **Lease expires.** ~20 seconds after the job was claimed (not after the kill), `lease_expires_at` passes.

5. **Any worker's next loop tick calls `reap_expired_leases()`** (`models.py`), which runs:
   ```sql
   UPDATE jobs SET state='pending' WHERE state='processing' AND lease_expires_at < now()
   ```
   This resets the job to `pending`. No special process or daemon is needed — any running worker (or a freshly started one) does this automatically.

6. **The job is now `pending` again** and will be claimed by the next available worker on its `claim_job()` call.

**Worst-case delay calculation:**
- Lease duration: 20 seconds (set in `worker.py → LEASE_SECONDS`)
- Poll interval: 2 seconds (set in `worker.py → POLL_INTERVAL`)
- Maximum time between loop ticks: 2 seconds
- **Worst case: 20 + 2 = 22 seconds** from the moment of the kill until the job is reaped and re-claimed.

(In practice, our Step 6 test showed ~51 seconds total — but that included 25 seconds of re-execution time for the `sleep 25` job itself. The recovery detection was at ~26s.)

---

## 3. Does `dlq retry` reset `attempts`? Why is that the right call?

**File:** `queuectl/models.py → dlq_retry()`, line ~200.

Yes. `dlq retry` sets `attempts=0` and `state='pending'`.

The reasoning, as implemented:

```python
conn.execute(
    "UPDATE jobs SET state='pending', attempts=0, ... WHERE id=? AND state='dead'",
    ...
)
```

A job reaches the DLQ because it failed `max_retries` times in a row. A human explicitly invoked `dlq retry` — this is an **intentional intervention**, signaling that the underlying issue (bad config, external service down, etc.) has likely been fixed. Resetting `attempts=0` gives the job its full retry budget back.

The alternative — keeping `attempts` at its maximum value — would mean the job goes straight back to `dead` on the very next failure, giving the operator no useful retries. That defeats the purpose of a DLQ retry.

---

## 4. What designs were considered and rejected for `worker stop`, and why?

**File:** `queuectl/worker.py → cmd_worker_stop()`

Three alternatives were considered:

**Option A: SIGTERM-only (rejected)**
`worker stop` sends `os.kill(pid, signal.SIGTERM)` to each worker PID. On Linux, SIGTERM is a catchable signal — the worker's handler sets a flag and exits gracefully after finishing the current job. **On Windows, `os.kill()` calls `TerminateProcess`, which kills the process immediately with no handler.** This was the original design and it failed Step 7 testing — the in-flight job was abandoned mid-execution. Rejected because it doesn't work cross-platform.

**Option B: External socket/pipe (rejected)**
Each worker could listen on a Unix domain socket or named pipe. `worker stop` connects and sends a "stop" message. Rejected because it adds non-trivial complexity (socket binding, port management, cleanup), introduces new failure modes (socket file leaks), and all this external state management is exactly what the SQLite DB is already handling for us.

**Option C: File-based flag (rejected)**
Create a `STOP` file in the project directory; workers poll for its existence. Rejected because: (1) the DB is already our shared state store — adding a parallel file-based flag is redundant, (2) it's harder to signal individual workers vs. all workers, and (3) cleanup is messier.

**Chosen: DB flag + conditional SIGTERM**
`worker stop` sets `stop_requested=1` in the `workers` table row for each PID. The worker checks this at the top of every loop iteration (before claiming a new job) and exits cleanly. On Linux, SIGTERM is also sent to wake a sleeping worker faster (it's caught by the signal handler which sets the same `shutdown_flag`). On Windows, SIGTERM is not sent (it would hard-kill). The DB flag alone is sufficient on all platforms.

---

## 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of the design survive unchanged and which break?

**Unchanged:**
- The entire DB schema (just add a `priority INTEGER DEFAULT 0` column to `jobs`)
- The retry/backoff/DLQ logic (`fail_job()`, `dlq_retry()`) — they don't touch ordering
- The lease and crash recovery mechanism — still works identically
- The `worker stop` / DB flag mechanism
- All CLI commands except `list` (which could filter by priority)

**What breaks / must change:**

1. **`claim_job()` in `models.py`** — The `ORDER BY created_at ASC` must become `ORDER BY priority DESC, created_at ASC`. This is a one-line change, but it changes the semantics of what "eligible" means.

2. **The `enqueue` command** — Must accept a `priority` field in the JSON spec and pass it through to `insert_job()`.

3. **`insert_job()` in `models.py`** — Must accept and store the `priority` value.

4. **Starvation risk** — With a simple `ORDER BY priority DESC` and a continuous stream of high-priority jobs, low-priority jobs may never run. The current design has no mechanism to age up a job's priority over time. This would need a new design decision (e.g., "boost priority by 1 every N seconds in the `failed` state").

The atomic claiming mechanism (`BEGIN IMMEDIATE`) survives unchanged — the priority just changes which row the SELECT picks, not the locking semantics.

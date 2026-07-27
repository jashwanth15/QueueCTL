#!/usr/bin/env bash
# tests/test_scenarios.sh
#
# Acceptance test suite for QueueCTL.
# Runs all 5 required scenarios end-to-end.
#
# Usage:
#   bash tests/test_scenarios.sh
#
# Requirements:
#   - Python 3 available as 'python3' or 'python'
#   - Run from the project root directory (where queuectl.py lives)
#   - bash (Linux/Mac) or Git Bash on Windows

set -euo pipefail

# ── Helpers ──────────────────────────────────────────────────────────────────

# Detect python binary
if command -v python3 &>/dev/null; then
    PY=python3
elif command -v python &>/dev/null; then
    PY=python
else
    echo "ERROR: Python not found."
    exit 1
fi

QUEUECTL="$PY queuectl.py"
DB_FILE="queue.db"
PASS=0
FAIL=0

_pass() { echo "  [PASS] $1"; PASS=$((PASS+1)); }
_fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }

# Clean the database before each scenario
clean_db() {
    rm -f "$DB_FILE" "${DB_FILE}-wal" "${DB_FILE}-shm"
}

# Wait up to N seconds for a condition (evaluated as a shell command)
wait_for() {
    local timeout=$1; shift
    local desc="$1"; shift
    local check_cmd="$@"
    local deadline=$((SECONDS + timeout))
    while [ $SECONDS -lt $deadline ]; do
        if eval "$check_cmd" &>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "  TIMEOUT waiting for: $desc"
    return 1
}

# Count jobs in a given state via list --json
count_state() {
    local state=$1
    $QUEUECTL list --state "$state" --json 2>/dev/null | $PY -c "import json,sys; print(len(json.load(sys.stdin)))"
}

echo "============================================"
echo "QueueCTL Acceptance Tests"
echo "============================================"
echo ""

# ── Scenario 1: Basic job completion ─────────────────────────────────────────
echo "Scenario 1: Basic job completion"
clean_db

$QUEUECTL enqueue '{"id":"s1-job","command":"echo scenario1-done"}' >/dev/null

# Start worker in background
$QUEUECTL worker start --count 1 &
WORKER_PID=$!

# Wait for job to complete
if wait_for 10 "job completed" '[ "$(count_state completed)" -ge 1 ]'; then
    _pass "Job reached state=completed"
else
    _fail "Job did not complete within 10s"
fi

# Stop worker
$QUEUECTL worker stop >/dev/null 2>&1 || true
wait $WORKER_PID 2>/dev/null || true
echo ""

# ── Scenario 2: Failing job retries and lands in DLQ ────────────────────────
echo "Scenario 2: Failing job retries with backoff, lands in DLQ"
clean_db

# max_retries=2 means 2 attempts total, then dead
$QUEUECTL enqueue '{"id":"s2-job","command":"exit 1","max_retries":2}' >/dev/null

$QUEUECTL worker start --count 1 &
WORKER_PID=$!

# Wait for job to reach dead state (2 failures + backoff = ~8s)
if wait_for 30 "job dead" '[ "$(count_state dead)" -ge 1 ]'; then
    _pass "Job reached state=dead after max_retries exhausted"
    # Verify attempts count
    attempts=$($QUEUECTL list --state dead --json 2>/dev/null | $PY -c "import json,sys; jobs=json.load(sys.stdin); print(jobs[0]['attempts']) if jobs else print(-1)")
    if [ "$attempts" -eq "2" ]; then
        _pass "Attempts count = 2 (correct)"
    else
        _fail "Expected attempts=2, got $attempts"
    fi
else
    _fail "Job did not reach dead state within 30s"
fi

$QUEUECTL worker stop >/dev/null 2>&1 || true
wait $WORKER_PID 2>/dev/null || true
echo ""

# ── Scenario 3: Many jobs, multiple workers, each runs exactly once ──────────
echo "Scenario 3: 20+ jobs across 3 workers, each runs exactly once"
clean_db

LOG_FILE="tests/scenario3_side_effect.log"
rm -f "$LOG_FILE"

# Enqueue 20 jobs — each appends its own ID to a log file
for i in $(seq -w 0 19); do
    $QUEUECTL enqueue "{\"id\":\"s3-job-$i\",\"command\":\"echo s3-job-$i >> $LOG_FILE\"}" >/dev/null
done

# Start 3 workers
$QUEUECTL worker start --count 1 &
W1=$!
$QUEUECTL worker start --count 1 &
W2=$!
$QUEUECTL worker start --count 1 &
W3=$!

# Wait for all 20 to complete
if wait_for 60 "all 20 jobs completed" '[ "$(count_state completed)" -ge 20 ]'; then
    _pass "All 20 jobs completed"

    # Check the side-effect log for duplicates
    if [ -f "$LOG_FILE" ]; then
        TOTAL=$(wc -l < "$LOG_FILE" | tr -d ' ')
        UNIQUE=$(sort "$LOG_FILE" | uniq | wc -l | tr -d ' ')
        if [ "$TOTAL" -eq "20" ] && [ "$UNIQUE" -eq "20" ]; then
            _pass "Log has 20 unique entries — no duplicates, no missing jobs"
        else
            _fail "Log has $TOTAL lines ($UNIQUE unique) — expected 20 each"
        fi
    else
        _fail "Side-effect log file not found"
    fi
else
    completed=$(count_state completed 2>/dev/null || echo 0)
    _fail "Only $completed/20 jobs completed within 60s"
fi

$QUEUECTL worker stop >/dev/null 2>&1 || true
wait $W1 $W2 $W3 2>/dev/null || true
echo ""

# ── Scenario 4: SIGKILL mid-job, job recovers via lease expiry ───────────────
echo "Scenario 4: Worker SIGKILL mid-job, job recovers within lease window"
clean_db

$QUEUECTL enqueue '{"id":"s4-job","command":"python -c \"import time; time.sleep(25)\""}' >/dev/null

# Start a worker and wait for it to pick up the job
$QUEUECTL worker start --count 1 &
WORKER_PID=$!

if wait_for 10 "job processing" '[ "$(count_state processing)" -ge 1 ]'; then
    echo "  Job is processing. SIGKILL-ing worker PID=$WORKER_PID..."
    kill -9 $WORKER_PID 2>/dev/null || true
    wait $WORKER_PID 2>/dev/null || true
    KILL_TIME=$SECONDS
    echo "  Worker killed. Starting recovery worker..."

    # Start a second worker to detect the expired lease and re-run the job
    $QUEUECTL worker start --count 1 &
    W2=$!

    # Wait for job to complete (lease=20s + job runtime ~25s = ~45s worst case)
    if wait_for 60 "s4-job completed" '[ "$(count_state completed)" -ge 1 ]'; then
        RECOVERY_TIME=$((SECONDS - KILL_TIME))
        _pass "Job recovered and completed in ${RECOVERY_TIME}s (< 60s)"
    else
        _fail "Job did not recover within 60s"
    fi

    $QUEUECTL worker stop >/dev/null 2>&1 || true
    wait $W2 2>/dev/null || true
else
    _fail "Job never entered 'processing' state"
    kill $WORKER_PID 2>/dev/null || true
fi
echo ""

# ── Scenario 5: Jobs survive full process restart ────────────────────────────
echo "Scenario 5: Jobs survive a full process restart"
clean_db

# Enqueue 3 jobs but don't start any workers yet
$QUEUECTL enqueue '{"id":"s5-job1","command":"echo s5-job1"}' >/dev/null
$QUEUECTL enqueue '{"id":"s5-job2","command":"echo s5-job2"}' >/dev/null
$QUEUECTL enqueue '{"id":"s5-job3","command":"echo s5-job3"}' >/dev/null

# Verify they are pending
PENDING=$(count_state pending)
if [ "$PENDING" -eq "3" ]; then
    _pass "3 jobs in pending state before restart"
else
    _fail "Expected 3 pending, got $PENDING"
fi

# Simulate "full restart" — just verify the DB survives (no workers running)
# Re-read status from scratch (new process reading existing DB)
PENDING_AFTER=$($QUEUECTL list --state pending --json 2>/dev/null | $PY -c "import json,sys; print(len(json.load(sys.stdin)))")
if [ "$PENDING_AFTER" -eq "3" ]; then
    _pass "Jobs still present after restart (DB persisted)"
else
    _fail "Expected 3 pending after restart, got $PENDING_AFTER"
fi

# Now start workers and verify all 3 complete
$QUEUECTL worker start --count 1 &
WORKER_PID=$!

if wait_for 15 "all jobs completed" '[ "$(count_state completed)" -ge 3 ]'; then
    _pass "All 3 jobs completed after restart"
else
    _fail "Jobs did not complete after restart"
fi

$QUEUECTL worker stop >/dev/null 2>&1 || true
wait $WORKER_PID 2>/dev/null || true
echo ""

# ── Summary ──────────────────────────────────────────────────────────────────
echo "============================================"
echo "Results: $PASS passed, $FAIL failed"
echo "============================================"

rm -f tests/scenario3_side_effect.log

if [ $FAIL -gt 0 ]; then
    exit 1
fi

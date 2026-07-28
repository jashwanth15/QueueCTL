"""
worker.py — Worker process loop, signal handling, and heartbeating.

A "worker" is a simple loop that:
  1. Reaps any stale leases (crash recovery)
  2. Claims one job atomically
  3. Runs the job via subprocess
  4. Marks it completed or failed
  5. Sleeps briefly and repeats

Multiple instances of this loop run as separate OS processes. Each
worker registers its PID in the 'workers' DB table. The 'worker stop'
command sets a stop_requested flag in that table row, which the worker
polls at the top of every loop — this is the cross-platform shutdown
mechanism. On Linux, SIGTERM/SIGINT are also handled for interactive use.
"""
import os
import sys
import time
import signal
import subprocess
import threading

from queuectl.db import get_connection, init_db
from queuectl.models import (
    claim_job, complete_job, fail_job, reap_expired_leases,
    register_worker, unregister_worker, heartbeat_worker,
    get_config,
)

# How long to sleep between poll attempts (when no job is available).
POLL_INTERVAL = 2  # seconds

# How long a job's lease lasts before it is considered crashed.
LEASE_SECONDS = 20


def _is_stop_requested(conn, pid):
    """
    Check the workers table to see if stop_requested has been set for this PID.
    This is the cross-platform shutdown signaling mechanism:
    'worker stop' writes to the DB; the worker reads it here.
    """
    row = conn.execute(
        "SELECT stop_requested FROM workers WHERE pid=?", (pid,)
    ).fetchone()
    # Row is None if the worker was already unregistered (e.g. another stop).
    return row is None or row["stop_requested"] == 1


def run_worker(worker_id):
    """
    Main worker loop. Runs in the foreground. Exits cleanly on SIGTERM/SIGINT.

    The 'shutdown_flag' is a threading.Event set by the signal handler.
    We check it after each job so we don't abandon a job mid-execution.
    """
    init_db()
    conn = get_connection()

    pid = os.getpid()
    register_worker(conn, pid)
    print(f"[Worker {worker_id}] Started (PID {pid})", file=sys.stderr)

    # Signal handlers set a flag instead of raising an exception,
    # so we always finish the current job before exiting.
    shutdown_flag = threading.Event()

    def handle_shutdown(signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        print(f"[Worker {worker_id}] {sig_name} received — will stop after current job.", file=sys.stderr)
        shutdown_flag.set()

    # signal.signal() only works in the main thread of the main interpreter.
    # When run as a proper separate OS process (the intended deployment), this
    # is always the main thread. We guard with try/except for testing flexibility.
    try:
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)
    except ValueError:
        # Not in main thread — skip signal handlers (e.g., unit test environments).
        pass

    # Start a background thread to update our heartbeat every 5 seconds.
    # This keeps our entry in the workers table fresh.
    def heartbeat_loop():
        while not shutdown_flag.is_set():
            try:
                heartbeat_worker(conn, pid)
            except Exception:
                pass
            time.sleep(5)

    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    # Read backoff-base from config.
    backoff_base = int(get_config(conn, "backoff-base", default="2"))

    try:
        while not shutdown_flag.is_set():
            # ── DB-BASED STOP CHECK ──
            # Check the workers table for a stop_requested flag.
            # This is how 'worker stop' signals us cross-platform:
            # it writes to the DB and we read it here.
            # This check runs even if the signal handler wasn't triggered.
            if _is_stop_requested(conn, pid):
                print(f"[Worker {worker_id}] Stop requested via DB flag — shutting down.", file=sys.stderr)
                break

            # ── CRASH RECOVERY ──
            # Before looking for new work, reset any jobs whose lease has expired.
            # This handles the case where a worker was killed mid-job.
            reap_expired_leases(conn)

            # ── CLAIM ──
            job = claim_job(conn, worker_id=str(worker_id), lease_seconds=LEASE_SECONDS)

            if job is None:
                # No work available; poll again after a short sleep.
                time.sleep(POLL_INTERVAL)
                continue

            print(f"[Worker {worker_id}] Running job '{job['id']}': {job['command']}", file=sys.stderr)

            # ── EXECUTE ──
            result = subprocess.run(job["command"], shell=True)

            if result.returncode == 0:
                # ── SUCCESS ──
                complete_job(conn, job["id"])
                print(f"[Worker {worker_id}] Job '{job['id']}' completed.", file=sys.stderr)
            else:
                # ── FAILURE ──
                # Re-read backoff-base each time in case config changed.
                backoff_base = int(get_config(conn, "backoff-base", default="2"))
                fail_job(conn, job["id"], backoff_base=backoff_base)
                print(f"[Worker {worker_id}] Job '{job['id']}' failed (exit {result.returncode}).", file=sys.stderr)

    finally:
        # Clean up: remove our PID from the workers table.
        try:
            unregister_worker(conn, pid)
        except Exception:
            pass
        conn.close()
        print(f"[Worker {worker_id}] Stopped cleanly.", file=sys.stderr)


def cmd_worker_start(args):
    """
    Start N worker processes IN THE FOREGROUND.

    We spawn (args.count - 1) child processes using os.fork() [Unix] or
    subprocess on Windows, then run the last worker in the main process.
    The main process blocks until all workers exit.
    """
    count = args.count

    if sys.platform == "win32":
        # Windows: spawn child worker processes as subprocesses.
        _start_workers_windows(count)
    else:
        # Unix: fork child processes.
        _start_workers_unix(count)


def _start_workers_unix(count):
    """Fork (count-1) children, run the last worker in the parent."""
    child_pids = []

    for i in range(1, count):  # workers 1..count-1 as children
        pid = os.fork()
        if pid == 0:
            # Child process: run worker i and exit.
            run_worker(i)
            os._exit(0)
        else:
            child_pids.append(pid)

    # Parent runs worker 0.
    try:
        run_worker(0)
    finally:
        # Wait for all children to exit.
        for pid in child_pids:
            try:
                os.waitpid(pid, 0)
            except Exception:
                pass


def _start_workers_windows(count):
    """
    On Windows, fork is not available. Spawn child workers as separate
    Python subprocesses using the same cli.py entry point with a special
    internal flag --_worker-id=N.
    """
    import subprocess as sp
    cli_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queuectl", "cli.py")
    children = []

    for i in range(1, count):
        proc = sp.Popen(
            [sys.executable, cli_path, "_run-worker", f"--worker-id={i}"],
            # Keep stdout/stderr connected to the terminal.
        )
        children.append(proc)

    # Main process runs worker 0.
    try:
        run_worker(0)
    finally:
        for proc in children:
            try:
                proc.wait()
            except Exception:
                pass


def cmd_worker_stop(args):
    """
    Stop all running workers gracefully.

    For each registered worker PID:
    - If the process IS alive: set stop_requested=1 so it exits cleanly
      after finishing its current job.
    - If the process is NOT alive (stale row from a crash or old session):
      delete the row immediately — no point setting a flag for a dead process.
    """
    init_db()
    conn = get_connection()
    from queuectl.models import get_all_workers
    workers = get_all_workers(conn)

    if not workers:
        print("No workers currently registered.")
        conn.close()
        return

    for w in workers:
        pid = w["pid"]

        # Probe if the process is actually alive without sending a real signal.
        # os.kill(pid, 0) raises OSError/ProcessLookupError if the PID is gone.
        process_alive = False
        try:
            os.kill(pid, 0)
            process_alive = True
        except (ProcessLookupError, OSError):
            process_alive = False

        if process_alive:
            # Process is running — set the DB flag so it exits after its
            # current job. The worker checks this at the top of every loop tick.
            with conn:
                conn.execute(
                    "UPDATE workers SET stop_requested=1 WHERE pid=?", (pid,)
                )
            print(f"Requested stop for worker PID {pid} (DB flag set).")

            # On Linux/Mac, also send SIGTERM to wake a sleeping worker faster.
            # Skipped on Windows because os.kill() there is TerminateProcess.
            if sys.platform != "win32":
                try:
                    os.kill(pid, signal.SIGTERM)
                except (ProcessLookupError, OSError):
                    pass
        else:
            # Process is dead — stale row from a previous session or crash.
            # Remove it from the DB so status shows clean state.
            with conn:
                conn.execute("DELETE FROM workers WHERE pid=?", (pid,))
            print(f"Removed stale worker PID {pid} (process no longer running).")

    conn.close()

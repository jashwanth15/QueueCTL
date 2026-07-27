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
command finds those PIDs and sends SIGTERM.
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
    Stop all running workers by sending SIGTERM to each PID in the workers table.

    This command is run from a DIFFERENT terminal than the workers.
    It discovers worker PIDs via the SQLite DB (the workers table),
    then uses os.kill() to send SIGTERM to each one.
    """
    init_db()
    conn = get_connection()
    from queuectl.models import get_all_workers
    workers = get_all_workers(conn)
    conn.close()

    if not workers:
        print("No workers currently registered.")
        return

    for w in workers:
        pid = w["pid"]
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent SIGTERM to worker PID {pid}.")
        except ProcessLookupError:
            print(f"PID {pid} not found (already exited?).", file=sys.stderr)
        except PermissionError:
            print(f"Permission denied to signal PID {pid}.", file=sys.stderr)

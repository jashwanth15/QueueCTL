"""
cli.py — Main entry point for the queuectl CLI.

Defines all subcommands using argparse and dispatches to the appropriate
handler in commands/ or worker.py.

Usage:
    python queuectl/cli.py <command> [options]
"""
import argparse
import sys
import os

# Make sure the project root is on the path so 'from queuectl.xxx import ...' works
# regardless of where the user runs the script from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuectl.commands import enqueue, status, list_cmd, dlq, config_cmd
from queuectl import worker as worker_mod


def main():
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="QueueCTL — CLI background job queue",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── enqueue ──────────────────────────────────────────────────────────────
    p_enqueue = subparsers.add_parser("enqueue", help="Add a new job")
    p_enqueue.add_argument("job_json", help='JSON job spec, e.g. \'{"id":"job1","command":"echo hi"}\'')
    p_enqueue.set_defaults(func=enqueue.run)

    # ── worker ───────────────────────────────────────────────────────────────
    p_worker = subparsers.add_parser("worker", help="Worker management")
    worker_sub = p_worker.add_subparsers(dest="worker_command", required=True)

    p_worker_start = worker_sub.add_parser("start", help="Start N worker processes (foreground)")
    p_worker_start.add_argument("--count", type=int, default=1, help="Number of workers to start (default: 1)")
    p_worker_start.set_defaults(func=worker_mod.cmd_worker_start)

    p_worker_stop = worker_sub.add_parser("stop", help="Gracefully stop all running workers")
    p_worker_stop.set_defaults(func=worker_mod.cmd_worker_stop)

    # ── status ───────────────────────────────────────────────────────────────
    p_status = subparsers.add_parser("status", help="Show job counts and active workers")
    p_status.set_defaults(func=status.run)

    # ── list ─────────────────────────────────────────────────────────────────
    p_list = subparsers.add_parser("list", help="List jobs by state")
    p_list.add_argument("--state", required=True,
                        choices=["pending", "processing", "completed", "failed", "dead"],
                        help="Job state to filter by")
    p_list.add_argument("--json", action="store_true", dest="json",
                        help="Output as JSON array (only JSON on stdout)")
    p_list.set_defaults(func=list_cmd.run)

    # ── dlq ──────────────────────────────────────────────────────────────────
    p_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_command", required=True)

    p_dlq_list = dlq_sub.add_parser("list", help="List all dead jobs")
    p_dlq_list.set_defaults(func=dlq.run_list)

    p_dlq_retry = dlq_sub.add_parser("retry", help="Re-enqueue a dead job")
    p_dlq_retry.add_argument("job_id", help="ID of the dead job to retry")
    p_dlq_retry.set_defaults(func=dlq.run_retry)

    # ── config ───────────────────────────────────────────────────────────────
    p_config = subparsers.add_parser("config", help="Manage configuration")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)

    p_config_set = config_sub.add_parser("set", help="Set a config value")
    p_config_set.add_argument("key", help="Config key (max-retries, backoff-base)")
    p_config_set.add_argument("value", help="Value to set")
    p_config_set.set_defaults(func=config_cmd.run_set)

    p_config_get = config_sub.add_parser("get", help="Get a config value")
    p_config_get.add_argument("key", help="Config key to read")
    p_config_get.set_defaults(func=config_cmd.run_get)

    # ── internal: _run-worker (used by Windows multi-worker spawning) ────────
    p_run_worker = subparsers.add_parser("_run-worker", help=argparse.SUPPRESS)
    p_run_worker.add_argument("--worker-id", type=int, default=0)
    p_run_worker.set_defaults(func=lambda a: worker_mod.run_worker(a.worker_id))

    # ── dispatch ─────────────────────────────────────────────────────────────
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

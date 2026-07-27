"""
commands/config_cmd.py — Handle 'queuectl config set <key> <value>'.
"""
import sys
from queuectl.db import get_connection, init_db
from queuectl.models import set_config, get_config

# These are the only keys we support changing.
VALID_KEYS = {"max-retries", "backoff-base"}


def run_set(args):
    """Persist a config key=value pair."""
    if args.key not in VALID_KEYS:
        print(f"Error: Unknown config key '{args.key}'. Valid keys: {', '.join(VALID_KEYS)}", file=sys.stderr)
        sys.exit(1)

    # Validate that the value is a positive integer.
    try:
        val = int(args.value)
        if val < 1:
            raise ValueError()
    except ValueError:
        print(f"Error: Value must be a positive integer, got '{args.value}'.", file=sys.stderr)
        sys.exit(1)

    init_db()
    conn = get_connection()
    set_config(conn, args.key, val)
    conn.close()
    print(f"Config set: {args.key} = {val}")
    print("Note: This applies to jobs enqueued after this change. Existing jobs are unaffected.")


def run_get(args):
    """Print the current value of a config key."""
    init_db()
    conn = get_connection()
    value = get_config(conn, args.key)
    conn.close()

    if value is None:
        print(f"Config key '{args.key}' not set (using default).")
    else:
        print(f"{args.key} = {value}")

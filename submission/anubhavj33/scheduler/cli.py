"""
Command-line entry point for the DAG job scheduler.

Usage:
    python -m scheduler.cli validate < input.json
    python -m scheduler.cli order < input.json
    python -m scheduler.cli stages [--workers N] < input.json
    python -m scheduler.cli critical-path < input.json
    python -m scheduler.cli downstream <job_id> < input.json
    python -m scheduler.cli summary < input.json
"""

from __future__ import annotations

import json
import sys

from .graph import ValidationError as GraphValidationError
from .scheduler import Scheduler


def _read_stdin() -> str:
    """Read all of stdin into a string."""
    if sys.stdin.isatty():
        print("Error: Pipe JSON input via stdin", file=sys.stderr)
        sys.exit(1)
    return sys.stdin.read()


def _make_scheduler() -> Scheduler:
    raw = _read_stdin()
    try:
        return Scheduler(raw)
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        print(f"Input error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_validate() -> None:
    scheduler = _make_scheduler()
    result = scheduler.validate()
    if result.is_valid:
        print("✓ Graph is valid (no cycles, no missing dependencies)")
    else:
        print("✗ Graph validation FAILED")
        for err in result.errors:
            print(f"  • {err}")
    print(json.dumps({
        "is_valid": result.is_valid,
        "cycles": result.cycles,
        "missing_dependencies": result.missing_dependencies,
        "errors": result.errors,
    }, indent=2))
    sys.exit(0 if result.is_valid else 1)


def cmd_order() -> None:
    scheduler = _make_scheduler()
    order = scheduler.execution_order()
    print("Execution order:")
    for i, jid in enumerate(order, 1):
        print(f"  {i}. {jid}")
    print(json.dumps({"execution_order": order}, indent=2))


def cmd_stages() -> None:
    worker_limit = 0
    if "--workers" in sys.argv:
        idx = sys.argv.index("--workers")
        if idx + 1 < len(sys.argv):
            try:
                worker_limit = int(sys.argv[idx + 1])
            except ValueError:
                print("Error: --workers requires an integer", file=sys.stderr)
                sys.exit(1)

    scheduler = _make_scheduler()
    stages = scheduler.parallel_stages(worker_limit)
    print("Parallel stages:")
    for s in stages:
        print(f"  Stage {s.level}: {', '.join(s.job_ids)}")
    print(json.dumps(
        [{"stage": s.level, "jobs": s.job_ids} for s in stages],
        indent=2,
    ))


def cmd_critical_path() -> None:
    scheduler = _make_scheduler()
    cp = scheduler.critical_path()
    print(f"Critical path: {' -> '.join(cp.path)}")
    print(f"Total time:    {cp.total_time}")
    print(json.dumps({
        "path": cp.path,
        "total_time": cp.total_time,
    }, indent=2))


def cmd_downstream() -> None:
    # job_id is the last positional argument (after the command name)
    job_id = sys.argv[-1] if len(sys.argv) > 2 else ""
    if not job_id or job_id.startswith("--"):
        print("Usage: scheduler.cli downstream <job_id>", file=sys.stderr)
        sys.exit(1)
    scheduler = _make_scheduler()
    downstream = scheduler.downstream_of(job_id)
    if downstream:
        print(f"Jobs downstream of '{job_id}':")
        for jid in downstream:
            print(f"  • {jid}")
    else:
        print(f"No jobs downstream of '{job_id}'.")
    print(json.dumps({"failed_job": job_id, "skipped": downstream}, indent=2))


def cmd_summary() -> None:
    scheduler = _make_scheduler()
    summary = scheduler.summary()
    print(json.dumps(summary, indent=2))


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, end="")
        sys.exit(1)

    command = sys.argv[1]

    # Remove leading '--' from command if present
    command = command.lstrip("-")

    commands = {
        "validate": cmd_validate,
        "order": cmd_order,
        "stages": cmd_stages,
        "critical-path": cmd_critical_path,
        "criticalpath": cmd_critical_path,
        "cp": cmd_critical_path,
        "downstream": cmd_downstream,
        "summary": cmd_summary,
    }

    handler = commands.get(command)
    if handler is None:
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__, end="")
        sys.exit(1)

    handler()


if __name__ == "__main__":
    main()
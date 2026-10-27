# PS5: Job Scheduler with Dependency Resolution (DAG)

A mini build/ETL orchestrator written in Python that processes job graphs
expressed as JSON. Each job has a **duration** and an optional **dependsOn** list
of upstream job IDs.

## Features — Milestones

| # | Milestone | Status |
|---|---|---|
| M1 | **Validate the graph** — detect cycles (reports the path) & missing dependencies | ✅ |
| M2 | **Topological sort** — produce a valid execution order | ✅ |
| M3 | **Parallel stages** — group jobs into stages with a configurable worker limit N | ✅ |
| M4 | **Critical path** — compute the minimal total completion time and the longest path | ✅ |
| M5 | **Failure simulation** — given a failed job, list every downstream job skipped | ✅ |

## Quick start

```bash
# Requires Python 3.9+
cd submission/anubhavj33

# Run tests
python -m pytest tests/ -v

# Run the CLI
python -m scheduler.cli summary < examples/simple.json
```

## Usage

### As a library

```python
from scheduler import Scheduler, JobGraph, ValidationError

# From a Python dict
jobs = [
    {"id": "A", "duration": 3, "dependsOn": []},
    {"id": "B", "duration": 2, "dependsOn": ["A"]},
    {"id": "C", "duration": 4, "dependsOn": ["A"]},
    {"id": "D", "duration": 1, "dependsOn": ["B", "C"]},
]

sched = Scheduler(jobs)

# M1 — Validate
validation = sched.validate()
print(validation.is_valid)          # True
print(validation.errors)            # []

# M2 — Execution order
order = sched.execution_order()     # ['A', 'B', 'C', 'D']

# M3 — Parallel stages (no worker limit)
stages = sched.parallel_stages()
for stage in stages:
    print(stage.level, stage.job_ids)

# M3 — With worker limit
stages = sched.parallel_stages(worker_limit=2)

# M4 — Critical path
cp = sched.critical_path()
print(cp.path)                      # ['A', 'C', 'D']
print(cp.total_time)                # 8

# M5 — Downstream jobs on failure
skipped = sched.downstream_of("B")  # ['D']
```

### From the command line

```bash
# Validate
python -m scheduler.cli validate < examples/simple.json

# Topological order
python -m scheduler.cli order < examples/simple.json

# Parallel stages
python -m scheduler.cli stages < examples/simple.json

# With worker limit
python -m scheduler.cli stages --workers 2 < examples/independent.json

# Critical path
python -m scheduler.cli critical-path < examples/simple.json

# Downstream impact
python -m scheduler.cli downstream A < examples/simple.json

# Full summary
python -m scheduler.cli summary < examples/simple.json
```

### Error scenarios

```bash
# Cycle detection
python -m scheduler.cli validate < examples/cycle.json

# Missing dependencies
python -m scheduler.cli validate < examples/missing_deps.json
```

## Project structure

```
submission/anubhavj33/
├── README.md
├── PROMPTS.md
├── setup.cfg                     # pytest config
├── requirements-dev.txt
├── scheduler/
│   ├── __init__.py               # Public API
│   ├── __main__.py               # python -m scheduler
│   ├── version.py
│   ├── models.py                 # Job, JobGraph data models
│   ├── graph.py                  # Core DAG algorithms (M1–M5)
│   ├── scheduler.py              # High-level Scheduler wrapper
│   └── cli.py                    # CLI entry point
├── tests/
│   └── test_scheduler.py         # 50+ tests covering all milestones
└── examples/
    ├── simple.json
    ├── complex.json
    ├── cycle.json
    ├── independent.json
    └── missing_deps.json
```

## Algorithm notes

- **Cycle detection**: DFS with white/gray/black colouring. On a back-edge,
  the full cycle path is reconstructed using a parent-pointer map.
- **Topological sort**: Kahn's algorithm (BFS on in-degree).
- **Parallel stages**: Compute the earliest stage for each job via a forward
  pass in topological order; split overflow when `worker_limit` is set.
- **Critical path**: Forward pass computes EST/EFT; sink with max EFT anchors
  the path; walk back via predecessor pointers to reconstruct.
- **Downstream propagation**: BFS over reverse-adjacency from the failed node.

## Running tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
# Over 50 tests covering every milestone and edge cases
```
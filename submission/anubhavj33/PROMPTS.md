# PROMPTS.md — AI Collaboration Effectiveness

This document records how AI agents (Claude, Copilot, etc.) can be most
effectively used with this project.  It is both a log and a guide.

## Effective prompts for this codebase

### 1. Initial generation of the scheduler

```
Build a Python DAG job scheduler. Requirements:
- Input: JSON list of jobs with id, duration, dependsOn
- M1: Validate graph — detect cycles (report path) and missing deps
- M2: Topological sort (Kahn's algorithm)
- M3: Parallel stages with configurable worker limit N
- M4: Critical path + minimum total completion time
- M5: Given a failed job, list all downstream jobs that would be skipped
- Must use clean dataclass models and proper error handling
- Structure as a proper Python package with tests
```

### 2. Adding a new feature to the graph algorithms

```
I want to add [feature] to the DAG scheduler. The relevant file is
scheduler/graph.py. The existing pattern is:
- Each milestone function takes a JobGraph and returns a typed dataclass
- Validation is handled via ValidationError exceptions
- Functions are pure (no side effects)
Follow the same pattern and add tests to tests/test_scheduler.py.
```

### 3. Debugging a test failure

```
The test [test_name] in tests/test_scheduler.py is failing.
Here's the error: [paste traceback]
What's the root cause and how should I fix it?
```

### 4. Running the full test suite

```
Run the test suite for the DAG scheduler. The package lives under
submission/anubhavj33/. Use pytest with verbose output.
```

## Design decisions (for AI context)

| Decision | Rationale |
|---|---|
| **Separate `graph.py` from `scheduler.py`** | Pure algorithm functions are independently testable; the scheduler is a thin convenience wrapper. |
| **Dataclass return types** (`ValidationResult`, `CriticalPathResult`, `Stage`) | Self-documenting; IDE-friendly autocomplete; serialisable to dict/JSON for the CLI. |
| **Validation before execution** | `topological_sort`, `parallel_stages`, `critical_path` all raise `ValidationError` if the graph is invalid — never silently produce wrong results. |
| **BFS for downstream** | O(V+E), straightforward; reverse-adjacency avoids scanning all jobs for each propagation. |
| **White/gray/black DFS for cycle detection** | Standard algorithm that reconstructs the full cycle path via parent pointers. |

## Known AI limitations

- The CLI uses `sys.stdin.read()` which doesn't work interactively — pipe JSON in.
- Worker-limit stage splitting uses a simple chunking strategy; optimal
  load-balancing is a harder problem (NP-hard in general) and is explicitly
  out of scope.
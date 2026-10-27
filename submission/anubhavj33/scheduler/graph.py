"""
Core graph algorithms for DAG job scheduling.

Implements:
  - M1: Cycle detection & missing dependency validation
  - M2: Topological sort
  - M3: Parallel stage grouping with worker limits
  - M4: Critical path computation
  - M5: Downstream failure propagation
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .models import JobGraph, ValidationError


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Result of graph validation.

    Attributes:
        is_valid: True if the graph has no cycles or missing deps.
        cycles: List of detected cycles, each expressed as a node-name path.
        missing_dependencies: Dict mapping job IDs to lists of missing dep IDs.
        errors: Flat list of all error messages.
    """

    is_valid: bool = True
    cycles: List[List[str]] = field(default_factory=list)
    missing_dependencies: Dict[str, List[str]] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# M1: Graph validation
# ---------------------------------------------------------------------------


def _build_adjacency(graph: JobGraph):
    """Build forward and reverse adjacency lists and validate IDs."""
    job_set = graph.job_ids_set
    fwd: Dict[str, List[str]] = {j.id: list(j.dependsOn) for j in graph.jobs}
    rev: Dict[str, List[str]] = {j.id: [] for j in graph.jobs}
    for j in graph.jobs:
        for dep in j.dependsOn:
            if dep in job_set:
                rev[dep].append(j.id)
    return fwd, rev, job_set


def find_cycle(graph: JobGraph) -> Optional[List[str]]:
    """Return one cycle (as a list of node IDs) or None if acyclic.

    Uses DFS with a path-tracking set (white/gray/black).
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {j.id: WHITE for j in graph.jobs}
    parent: Dict[str, Optional[str]] = {}

    fwd, _rev, _ = _build_adjacency(graph)

    def dfs(node: str) -> Optional[List[str]]:
        color[node] = GRAY
        for dep in fwd.get(node, []):
            if dep not in color:
                continue
            if color[dep] == GRAY:
                # Back edge found — reconstruct the cycle
                cycle = [dep, node]
                cur = node
                while cur != dep:
                    cur = parent.get(cur)
                    if cur is None:
                        break
                    cycle.append(cur)
                return cycle
            if color[dep] == WHITE:
                parent[dep] = node
                result = dfs(dep)
                if result is not None:
                    return result
        color[node] = BLACK
        return None

    for j in graph.jobs:
        if color[j.id] == WHITE:
            result = dfs(j.id)
            if result is not None:
                # Normalise so cycle starts with the smallest node
                # and is a *simple cycle* — reverse since we built it backwards
                cycle = list(reversed(result))
                # Rotate to smallest element
                min_idx = min(range(len(cycle)), key=lambda i: cycle[i])
                cycle = cycle[min_idx:] + cycle[:min_idx]
                return cycle
    return None


def validate_graph(graph: JobGraph) -> ValidationResult:
    """Validate the DAG: check for cycles and missing dependencies.

    Returns a ValidationResult summarising all problems found.
    """
    result = ValidationResult()

    # -- Check missing dependencies -------------------------------------------
    job_set = graph.job_ids_set
    has_missing = False
    for j in graph.jobs:
        missing = [dep for dep in j.dependsOn if dep not in job_set]
        if missing:
            result.missing_dependencies[j.id] = missing
            result.errors.append(
                f"Job '{j.id}' depends on unknown job(s): {', '.join(missing)}"
            )
            has_missing = True

    if has_missing:
        result.is_valid = False

    # -- Check for cycles -----------------------------------------------------
    cycle = find_cycle(graph)
    if cycle is not None:
        result.cycles.append(cycle)
        result.errors.append(
            f"Cycle detected: {' -> '.join(cycle)}"
        )
        result.is_valid = False

    return result


# ---------------------------------------------------------------------------
# M2: Topological sort
# ---------------------------------------------------------------------------


def topological_sort(graph: JobGraph) -> List[str]:
    """Return a topologically-sorted list of job IDs (Kahn's algorithm).

    Raises:
        ValidationError: If the graph is not a valid DAG.
    """
    # Validate first — includes cycle detection & missing deps
    validation = validate_graph(graph)
    if not validation.is_valid:
        raise ValidationError(
            "Cannot sort: graph has validation errors",
            errors=validation.errors,
        )

    # Build in-degree map
    in_degree: Dict[str, int] = {j.id: 0 for j in graph.jobs}
    fwd, _rev, job_set = _build_adjacency(graph)

    for j in graph.jobs:
        for dep in j.dependsOn:
            if dep in job_set:
                in_degree[j.id] = in_degree.get(j.id, 0) + 1

    # Queue jobs with no dependencies
    queue: deque = deque(
        [jid for jid, deg in in_degree.items() if deg == 0]
    )
    order: List[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        # Find jobs that depend on this node (reverse edges)
        for j in graph.jobs:
            if node in j.dependsOn:
                in_degree[j.id] -= 1
                if in_degree[j.id] == 0:
                    queue.append(j.id)

    # Also account for edges from jobs that reference non-existent jobs
    # (already caught by validate_graph, but be safe)
    if len(order) != len(graph.jobs):
        # Should not happen if validation passed, but guard anyway
        missing = set(graph.job_ids) - set(order)
        raise ValidationError(
            f"Topological sort could not order {len(missing)} job(s): "
            f"{', '.join(sorted(missing))}. Possible cycle or disconnected dep."
        )

    return order


# ---------------------------------------------------------------------------
# M3: Parallel stages
# ---------------------------------------------------------------------------


@dataclass
class Stage:
    """A group of jobs that can execute in parallel.

    Attributes:
        level: Zero-based stage index.
        job_ids: Jobs assigned to this stage.
    """

    level: int
    job_ids: List[str]


def parallel_stages(
    graph: JobGraph,
    worker_limit: int = 0,
) -> List[Stage]:
    """Group jobs into parallel execution stages.

    Each stage contains jobs whose dependencies are all satisfied by earlier
    stages.  When *worker_limit* > 0, stages are capped so no stage has more
    than *worker_limit* jobs; overflow jobs spill into the next stage.

    Raises:
        ValidationError: If the graph is not a valid DAG.
    """
    if worker_limit < 0:
        raise ValueError("worker_limit must be non-negative")

    topo = topological_sort(graph)

    # Map job -> its level (earliest stage it can run in)
    level_of: Dict[str, int] = {}

    # First pass: compute stage level for each job in topological order
    for job_id in topo:
        job = graph.get_job(job_id)
        if not job:
            continue
        if not job.dependsOn:
            level_of[job_id] = 0
        else:
            max_dep_level = max(
                level_of.get(dep, 0) for dep in job.dependsOn
                if graph.get_job(dep) is not None
            )
            level_of[job_id] = max_dep_level + 1

    if not level_of:
        return []

    if worker_limit == 0:
        # No limit — one stage per level
        max_level = max(level_of.values())
        stages = []
        for lvl in range(max_level + 1):
            jobs_at_level = [jid for jid in topo if level_of.get(jid) == lvl]
            if jobs_at_level:
                stages.append(Stage(level=lvl, job_ids=jobs_at_level))
        return stages

    # Worker limit — spill excess jobs to next stages
    return _assign_stages_with_limit(topo, level_of, worker_limit)


def _assign_stages_with_limit(
    topo: List[str],
    level_of: Dict[str, int],
    worker_limit: int,
) -> List[Stage]:
    """Assign jobs to stages respecting the worker limit."""
    max_level = max(level_of.values())
    raw_groups: List[List[str]] = [
        [jid for jid in topo if level_of.get(jid) == lvl]
        for lvl in range(max_level + 1)
    ]

    stages: List[Stage] = []
    level = 0

    for group in raw_groups:
        # Split the level's jobs into worker_limit-sized chunks
        for i in range(0, len(group), worker_limit):
            stages.append(Stage(level=level, job_ids=group[i:i + worker_limit]))
            level += 1

    return stages


# ---------------------------------------------------------------------------
# M4: Critical path
# ---------------------------------------------------------------------------


@dataclass
class CriticalPathResult:
    """Result of critical-path analysis.

    Attributes:
        path: List of job IDs forming the critical path (earliest -> latest).
        total_time: Sum of durations along the critical path (minimum
                    total completion time for the whole DAG).
        earliest_start: Dict mapping job ID -> earliest start time.
        earliest_finish: Dict mapping job ID -> earliest finish time.
    """

    path: List[str]
    total_time: int
    earliest_start: Dict[str, int] = field(default_factory=dict)
    earliest_finish: Dict[str, int] = field(default_factory=dict)


def critical_path(graph: JobGraph) -> CriticalPathResult:
    """Compute the critical path through the job DAG.

    Returns earliest-start / earliest-finish times for every job, the
    critical path (as a list of job IDs) and the total time.

    Raises:
        ValidationError: If the graph is not a valid DAG.
    """
    topo = topological_sort(graph)
    _fwd, rev, job_set = _build_adjacency(graph)

    est: Dict[str, int] = {}  # earliest start time
    eft: Dict[str, int] = {}  # earliest finish time
    predecessor: Dict[str, Optional[str]] = {}  # for path reconstruction

    # Forward pass in topological order
    for job_id in topo:
        job = graph.get_job(job_id)
        if not job:
            continue

        # EST = max EFT of all dependencies
        dep_times = [
            eft.get(dep, 0) for dep in job.dependsOn
            if dep in job_set
        ]
        est[job_id] = max(dep_times) if dep_times else 0
        eft[job_id] = est[job_id] + job.duration

        # Track which dependency gave the latest finish
        if dep_times:
            best_dep = max(
                (dep for dep in job.dependsOn if dep in job_set),
                key=lambda d: eft.get(d, 0),
            )
            predecessor[job_id] = best_dep

    # Find sink job(s) — the one with the maximum EFT
    if not eft:
        return CriticalPathResult(path=[], total_time=0, earliest_start={}, earliest_finish={})

    sink = max(eft, key=lambda jid: eft[jid])  # type: ignore[arg-type]
    total = eft[sink]

    # Walk back from sink to source to reconstruct critical path
    path: List[str] = []
    cur: Optional[str] = sink
    while cur is not None:
        path.append(cur)
        cur = predecessor.get(cur)
    path.reverse()

    return CriticalPathResult(
        path=path,
        total_time=total,
        earliest_start=est,
        earliest_finish=eft,
    )


# ---------------------------------------------------------------------------
# M5: Downstream failure propagation
# ---------------------------------------------------------------------------


def downstream_jobs(graph: JobGraph, failed_job_id: str) -> List[str]:
    """Return all jobs that are transitively downstream of *failed_job_id*.

    These jobs would be skipped if the given job fails.
    The failed job itself is *not* included.

    Raises:
        ValidationError: If *failed_job_id* is not in the graph.
    """
    if graph.get_job(failed_job_id) is None:
        raise ValidationError(
            f"Job '{failed_job_id}' not found in graph — cannot compute downstream impact."
        )

    fwd, rev, job_set = _build_adjacency(graph)
    visited: set = {failed_job_id}
    queue: deque = deque([failed_job_id])

    while queue:
        current = queue.popleft()
        for job_id in rev.get(current, []):
            if job_id not in visited:
                visited.add(job_id)
                queue.append(job_id)

    visited.discard(failed_job_id)
    return list(visited)
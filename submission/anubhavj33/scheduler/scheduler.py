"""
High-level scheduler that orchestrates validation, ordering, and execution
simulation of a DAG job graph.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from .graph import (
    CriticalPathResult,
    Stage,
    ValidationResult,
    critical_path,
    downstream_jobs,
    parallel_stages,
    topological_sort,
    validate_graph,
)
from .models import JobGraph, SchedulerError, ValidationError


class Scheduler:
    """Convenience wrapper around the DAG job scheduler algorithms.

    Accepts jobs as a JSON string, a Python dict/list, or a JobGraph.
    """

    def __init__(self, data: Union[str, Dict[str, Any], List[Any], JobGraph]):
        if isinstance(data, JobGraph):
            self.graph = data
        else:
            self.graph = JobGraph.from_input(data)

    # -- M1 ----------------------------------------------------------------

    def validate(self) -> ValidationResult:
        """Validate the job graph (cycle & missing-dep check)."""
        return validate_graph(self.graph)

    # -- M2 ----------------------------------------------------------------

    def execution_order(self) -> List[str]:
        """Return a valid topological execution order."""
        return topological_sort(self.graph)

    # -- M3 ----------------------------------------------------------------

    def parallel_stages(
        self, worker_limit: int = 0
    ) -> List[Stage]:
        """Group jobs into parallel execution stages.

        Args:
            worker_limit: Max jobs per stage. 0 = unlimited.
        """
        return parallel_stages(self.graph, worker_limit)

    # -- M4 ----------------------------------------------------------------

    def critical_path(self) -> CriticalPathResult:
        """Return critical-path analysis."""
        return critical_path(self.graph)

    # -- M5 ----------------------------------------------------------------

    def downstream_of(self, job_id: str) -> List[str]:
        """List all jobs downstream of *job_id* (excludes the job itself)."""
        return downstream_jobs(self.graph, job_id)

    # -- Convenience -------------------------------------------------------

    def summary(self) -> Dict[str, Any]:
        """Return a human-friendly summary of the job graph.

        Includes validation, order, stages, critical path, and total time.
        """
        validation = self.validate()

        result: Dict[str, Any] = {
            "num_jobs": len(self.graph),
            "valid": validation.is_valid,
        }

        if not validation.is_valid:
            result["errors"] = validation.errors
            return result

        result["execution_order"] = self.execution_order()

        stages = self.parallel_stages()
        result["stages"] = [
            {"stage": s.level, "jobs": s.job_ids} for s in stages
        ]

        cp = self.critical_path()
        result["critical_path"] = cp.path
        result["total_time"] = cp.total_time

        return result
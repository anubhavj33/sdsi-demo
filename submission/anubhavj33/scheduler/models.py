"""
Data models for the DAG job scheduler.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class SchedulerError(Exception):
    """Base exception for scheduler errors."""


class ValidationError(SchedulerError):
    """Raised when job graph validation fails."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        super().__init__(message)
        self.errors = errors or []


class JobNotFoundError(SchedulerError):
    """Raised when a referenced job ID does not exist."""


@dataclass
class Job:
    """Represents a single job in the DAG.

    Attributes:
        id: Unique identifier for the job.
        duration: Non-negative integer representing processing time.
        dependsOn: List of job IDs this job depends on.
    """

    id: str
    duration: int
    dependsOn: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Job":
        """Create a Job from a dictionary.

        Raises:
            TypeError: If the input is not a dict or fields have wrong types.
            ValueError: If required fields are missing or invalid.
        """
        if not isinstance(d, dict):
            raise TypeError(f"Expected dict, got {type(d).__name__}")

        job_id = d.get("id")
        if not job_id:
            raise ValueError("Job must have a non-empty 'id' field")
        if not isinstance(job_id, str):
            raise TypeError(f"Job 'id' must be a string, got {type(job_id).__name__}")

        duration = d.get("duration")
        if duration is None:
            raise ValueError(f"Job '{job_id}' must have a 'duration' field")
        if not isinstance(duration, int) or isinstance(duration, bool):
            raise TypeError(
                f"Job '{job_id}' duration must be an integer, "
                f"got {type(duration).__name__}"
            )
        if duration < 0:
            raise ValueError(
                f"Job '{job_id}' duration must be non-negative, got {duration}"
            )

        depends_on = d.get("dependsOn", [])
        if not isinstance(depends_on, list):
            raise TypeError(
                f"Job '{job_id}' dependsOn must be a list, "
                f"got {type(depends_on).__name__}"
            )
        if not all(isinstance(dep, str) for dep in depends_on):
            raise TypeError(
                f"Job '{job_id}' dependsOn entries must be strings"
            )

        return cls(id=job_id, duration=duration, dependsOn=list(depends_on))


@dataclass
class JobGraph:
    """A collection of jobs forming a directed acyclic graph.

    Attributes:
        jobs: List of Job objects.
    """

    jobs: List[Job]

    def __post_init__(self):
        self._by_id: Dict[str, Job] = {j.id: j for j in self.jobs}

    @classmethod
    def from_json(cls, json_str: str) -> "JobGraph":
        """Parse a JSON string into a JobGraph."""
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Any) -> "JobGraph":
        """Create a JobGraph from a dict (single job) or list of dicts."""
        if isinstance(data, dict):
            return cls(jobs=[Job.from_dict(data)])
        if isinstance(data, list):
            return cls(jobs=[Job.from_dict(j) for j in data])
        raise TypeError(f"Expected dict or list, got {type(data).__name__}")

    @classmethod
    def from_input(cls, data: Any) -> "JobGraph":
        """Create a JobGraph from a JSON string, dict, or list."""
        if isinstance(data, str):
            return cls.from_json(data)
        return cls.from_dict(data)

    def get_job(self, job_id: str) -> Optional[Job]:
        """Look up a job by ID. Returns None if not found."""
        return self._by_id.get(job_id)

    @property
    def job_ids(self) -> List[str]:
        """Return the list of all job IDs in insertion order."""
        return [j.id for j in self.jobs]

    @property
    def job_ids_set(self) -> set:
        """Return the set of all job IDs."""
        return set(self._by_id.keys())

    def to_dict(self) -> List[Dict[str, Any]]:
        """Serialize to a list of dicts."""
        return [
            {"id": j.id, "duration": j.duration, "dependsOn": list(j.dependsOn)}
            for j in self.jobs
        ]

    def __len__(self) -> int:
        return len(self.jobs)
"""
Comprehensive tests for the DAG job scheduler (PS5).
"""

from __future__ import annotations

import json
import math
import pytest

from scheduler.models import Job, JobGraph, ValidationError, SchedulerError
from scheduler.graph import (
    ValidationResult,
    CriticalPathResult,
    Stage,
    find_cycle,
    validate_graph,
    topological_sort,
    parallel_stages,
    critical_path,
    downstream_jobs,
)
from scheduler.scheduler import Scheduler


# ======================================================================
#  Fixtures
# ======================================================================

SIMPLE_JOBS = [
    {"id": "A", "duration": 3, "dependsOn": []},
    {"id": "B", "duration": 2, "dependsOn": ["A"]},
    {"id": "C", "duration": 4, "dependsOn": ["A"]},
    {"id": "D", "duration": 1, "dependsOn": ["B", "C"]},
]

DIAMOND_JOBS = [
    {"id": "A", "duration": 5, "dependsOn": []},
    {"id": "B", "duration": 3, "dependsOn": ["A"]},
    {"id": "C", "duration": 2, "dependsOn": ["A"]},
    {"id": "D", "duration": 4, "dependsOn": ["B", "C"]},
]

CHAIN_JOBS = [
    {"id": "A", "duration": 1, "dependsOn": []},
    {"id": "B", "duration": 2, "dependsOn": ["A"]},
    {"id": "C", "duration": 3, "dependsOn": ["B"]},
    {"id": "D", "duration": 4, "dependsOn": ["C"]},
]

WIDE_JOBS = [
    {"id": "A", "duration": 1, "dependsOn": []},
    {"id": "B", "duration": 2, "dependsOn": []},
    {"id": "C", "duration": 3, "dependsOn": []},
    {"id": "D", "duration": 4, "dependsOn": []},
]

CYCLIC_JOBS = [
    {"id": "X", "duration": 1, "dependsOn": ["Y"]},
    {"id": "Y", "duration": 2, "dependsOn": ["Z"]},
    {"id": "Z", "duration": 3, "dependsOn": ["X"]},
]

SELF_LOOP_JOBS = [
    {"id": "A", "duration": 1, "dependsOn": ["A"]},
]

MISSING_DEP_JOBS = [
    {"id": "A", "duration": 1, "dependsOn": ["B"]},
    {"id": "C", "duration": 2, "dependsOn": ["D", "E"]},
]

SINGLE_JOB = [{"id": "only", "duration": 10, "dependsOn": []}]

COMPLEX_JOBS = [
    {"id": "A", "duration": 5, "dependsOn": []},
    {"id": "B", "duration": 6, "dependsOn": ["A"]},
    {"id": "C", "duration": 3, "dependsOn": ["A"]},
    {"id": "D", "duration": 2, "dependsOn": ["B", "C"]},
    {"id": "E", "duration": 4, "dependsOn": ["B"]},
    {"id": "F", "duration": 1, "dependsOn": ["D", "E"]},
    {"id": "G", "duration": 7, "dependsOn": ["F"]},
]

DEEP_FORK_JOIN = [
    {"id": "A", "duration": 2, "dependsOn": []},
    {"id": "B", "duration": 3, "dependsOn": ["A"]},
    {"id": "C", "duration": 5, "dependsOn": ["A"]},
    {"id": "D", "duration": 1, "dependsOn": ["B"]},
    {"id": "E", "duration": 4, "dependsOn": ["C", "D"]},
    {"id": "F", "duration": 2, "dependsOn": ["E"]},
    {"id": "G", "duration": 3, "dependsOn": ["E"]},
    {"id": "H", "duration": 1, "dependsOn": ["F", "G"]},
]

MANY_INDEPENDENT = [
    {"id": f"J{i}", "duration": i, "dependsOn": []}
    for i in range(1, 101)
]

MANY_CHAINED = []
for i in range(100):
    dep = [f"J{i-1}"] if i > 0 else []
    MANY_CHAINED.append({"id": f"J{i}", "duration": 1, "dependsOn": dep})


# ======================================================================
#  Model tests
# ======================================================================


class TestJobModel:
    def test_from_dict_minimal(self):
        job = Job.from_dict({"id": "a", "duration": 5})
        assert job.id == "a"
        assert job.duration == 5
        assert job.dependsOn == []

    def test_from_dict_with_deps(self):
        job = Job.from_dict({"id": "a", "duration": 5, "dependsOn": ["b", "c"]})
        assert job.dependsOn == ["b", "c"]

    def test_from_dict_missing_id(self):
        with pytest.raises(ValueError, match="non-empty.*id"):
            Job.from_dict({"duration": 5})

    def test_from_dict_empty_id(self):
        with pytest.raises(ValueError, match="non-empty.*id"):
            Job.from_dict({"id": "", "duration": 5})

    def test_from_dict_non_string_id(self):
        with pytest.raises(TypeError, match="id.*string"):
            Job.from_dict({"id": 42, "duration": 5})

    def test_from_dict_missing_duration(self):
        with pytest.raises(ValueError, match="duration"):
            Job.from_dict({"id": "a"})

    def test_from_dict_negative_duration(self):
        with pytest.raises(ValueError, match="non-negative"):
            Job.from_dict({"id": "a", "duration": -1})

    def test_from_dict_float_duration(self):
        with pytest.raises(TypeError, match="duration.*integer"):
            Job.from_dict({"id": "a", "duration": 1.5})

    def test_from_dict_bool_duration(self):
        with pytest.raises(TypeError, match="duration.*integer"):
            Job.from_dict({"id": "a", "duration": True})

    def test_from_dict_zero_duration(self):
        job = Job.from_dict({"id": "a", "duration": 0})
        assert job.duration == 0

    def test_from_dict_non_list_dependsOn(self):
        with pytest.raises(TypeError, match="dependsOn.*list"):
            Job.from_dict({"id": "a", "duration": 1, "dependsOn": "b"})

    def test_from_dict_non_string_dep(self):
        with pytest.raises(TypeError, match="dependsOn.*strings"):
            Job.from_dict({"id": "a", "duration": 1, "dependsOn": [42]})

    def test_from_dict_not_a_dict(self):
        with pytest.raises(TypeError, match="dict"):
            Job.from_dict("not a dict")

    def test_from_dict_none(self):
        with pytest.raises(TypeError, match="dict"):
            Job.from_dict(None)


class TestJobGraphModel:
    def test_from_json_string(self):
        graph = JobGraph.from_json('[{"id":"A","duration":1}]')
        assert len(graph) == 1
        assert graph.get_job("A") is not None

    def test_from_json_invalid(self):
        with pytest.raises(ValueError, match="Invalid JSON"):
            JobGraph.from_json("not json")

    def test_from_dict_single_job(self):
        graph = JobGraph.from_dict({"id": "A", "duration": 5})
        assert len(graph) == 1

    def test_from_dict_list(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        assert len(graph) == 4

    def test_from_dict_invalid(self):
        with pytest.raises(TypeError, match="dict or list"):
            JobGraph.from_dict(42)

    def test_from_input_string(self):
        graph = JobGraph.from_input('[{"id":"A","duration":1}]')
        assert len(graph) == 1

    def test_from_input_dict(self):
        graph = JobGraph.from_input({"id": "A", "duration": 1})
        assert len(graph) == 1

    def test_from_input_list(self):
        graph = JobGraph.from_input(SIMPLE_JOBS)
        assert len(graph) == 4

    def test_get_job_missing(self):
        graph = JobGraph.from_dict(SINGLE_JOB)
        assert graph.get_job("nonexistent") is None

    def test_to_dict(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        serialised = graph.to_dict()
        assert len(serialised) == 4
        assert serialised[0]["id"] == "A"

    def test_job_ids(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        assert graph.job_ids == ["A", "B", "C", "D"]

    def test_job_ids_set(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        assert graph.job_ids_set == {"A", "B", "C", "D"}


# ======================================================================
#  M1: Validate graph — cycle detection & missing dependencies
# ======================================================================


class TestCycleDetection:
    def test_simple_cycle(self):
        graph = JobGraph.from_dict(CYCLIC_JOBS)
        cycle = find_cycle(graph)
        assert cycle is not None
        # Should contain X, Y, Z in some permutation
        assert set(cycle) == {"X", "Y", "Z"}
        assert len(cycle) == 3

    def test_self_loop(self):
        graph = JobGraph.from_dict(SELF_LOOP_JOBS)
        cycle = find_cycle(graph)
        assert cycle is not None
        assert "A" in cycle

    def test_acyclic(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        assert find_cycle(graph) is None

    def test_single_job_no_cycle(self):
        graph = JobGraph.from_dict(SINGLE_JOB)
        assert find_cycle(graph) is None

    def test_empty_graph_has_no_cycle(self):
        graph = JobGraph.from_dict([])
        assert find_cycle(graph) is None

    def test_diamond_no_cycle(self):
        graph = JobGraph.from_dict(DIAMOND_JOBS)
        assert find_cycle(graph) is None

    def test_chain_no_cycle(self):
        graph = JobGraph.from_dict(CHAIN_JOBS)
        assert find_cycle(graph) is None


class TestValidateGraph:
    def test_valid_graph(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        result = validate_graph(graph)
        assert result.is_valid
        assert result.errors == []

    def test_cycle_detected(self):
        graph = JobGraph.from_dict(CYCLIC_JOBS)
        result = validate_graph(graph)
        assert not result.is_valid
        assert len(result.cycles) == 1
        assert any("cycle" in e.lower() for e in result.errors)

    def test_missing_dependencies(self):
        graph = JobGraph.from_dict(MISSING_DEP_JOBS)
        result = validate_graph(graph)
        assert not result.is_valid
        assert "B" in result.missing_dependencies.get("A", [])
        assert "D" in result.missing_dependencies.get("C", [])
        assert "E" in result.missing_dependencies.get("C", [])

    def test_cycle_and_missing_deps(self):
        jobs = [
            {"id": "A", "duration": 1, "dependsOn": ["B"]},
            {"id": "B", "duration": 2, "dependsOn": ["C"]},
            {"id": "C", "duration": 3, "dependsOn": ["A", "MISSING"]},
        ]
        graph = JobGraph.from_dict(jobs)
        result = validate_graph(graph)
        assert not result.is_valid
        assert len(result.cycles) == 1
        assert "MISSING" in result.missing_dependencies.get("C", [])

    def test_all_independent_valid(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        result = validate_graph(graph)
        assert result.is_valid


# ======================================================================
#  M2: Topological sort
# ======================================================================


class TestTopologicalSort:
    def test_basic_order(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        order = topological_sort(graph)
        assert order[0] == "A"  # A has no deps
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("D")
        assert order.index("C") < order.index("D")

    def test_chain_order(self):
        graph = JobGraph.from_dict(CHAIN_JOBS)
        order = topological_sort(graph)
        assert order == ["A", "B", "C", "D"]

    def test_wide_order(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        order = topological_sort(graph)
        assert set(order) == {"A", "B", "C", "D"}

    def test_single_job(self):
        graph = JobGraph.from_dict(SINGLE_JOB)
        assert topological_sort(graph) == ["only"]

    def test_empty_list(self):
        graph = JobGraph.from_dict([])
        assert topological_sort(graph) == []

    def test_cyclic_raises(self):
        graph = JobGraph.from_dict(CYCLIC_JOBS)
        with pytest.raises(ValidationError, match="validation"):
            topological_sort(graph)

    def test_missing_dep_raises(self):
        graph = JobGraph.from_dict(MISSING_DEP_JOBS)
        with pytest.raises(ValidationError, match="validation"):
            topological_sort(graph)

    def test_preserves_ordering_with_fork_join(self):
        graph = JobGraph.from_dict(COMPLEX_JOBS)
        order = topological_sort(graph)
        assert order[0] == "A"
        for job_id in ["B", "C"]:
            assert order.index("A") < order.index(job_id)
        assert order.index("B") < order.index("D")
        assert order.index("B") < order.index("E")
        assert order.index("D") < order.index("F")
        assert order.index("E") < order.index("F")
        assert order.index("F") < order.index("G")

    @pytest.mark.skipif("MANY_INDEPENDENT" == "", reason="fixture not bound")
    def test_many_independent(self):
        # Use a fresh graph since M ANY_INDEPENDENT is a module-level list
        jobs = [{"id": f"J{i}", "duration": 1, "dependsOn": []} for i in range(50)]
        graph = JobGraph.from_dict(jobs)
        order = topological_sort(graph)
        assert len(order) == 50
        assert set(order) == {f"J{i}" for i in range(50)}

    def test_many_chained(self):
        jobs = [{"id": f"J{i}", "duration": 1, "dependsOn": [f"J{i-1}"] if i > 0 else []} for i in range(50)]
        graph = JobGraph.from_dict(jobs)
        order = topological_sort(graph)
        assert order == [f"J{i}" for i in range(50)]


# ======================================================================
#  M3: Parallel stages
# ======================================================================


class TestParallelStages:
    def test_simple_stages(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        stages = parallel_stages(graph)
        assert len(stages) == 3
        assert stages[0].job_ids == ["A"]
        assert set(stages[1].job_ids) == {"B", "C"}
        assert stages[2].job_ids == ["D"]

    def test_chain_stages(self):
        graph = JobGraph.from_dict(CHAIN_JOBS)
        stages = parallel_stages(graph)
        assert len(stages) == 4
        for i, s in enumerate(stages):
            assert s.job_ids == [CHAIN_JOBS[i]["id"]]

    def test_wide_stages(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        stages = parallel_stages(graph)
        assert len(stages) == 1  # all independent, parallel
        assert len(stages[0].job_ids) == 4

    def test_single_job_stage(self):
        graph = JobGraph.from_dict(SINGLE_JOB)
        stages = parallel_stages(graph)
        assert len(stages) == 1
        assert stages[0].job_ids == ["only"]

    def test_empty_graph(self):
        graph = JobGraph.from_dict([])
        assert parallel_stages(graph) == []

    def test_worker_limit_basic(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        stages = parallel_stages(graph, worker_limit=2)
        assert len(stages) == 2
        assert len(stages[0].job_ids) == 2
        assert len(stages[1].job_ids) == 2

    def test_worker_limit_one(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        stages = parallel_stages(graph, worker_limit=1)
        assert len(stages) == 4
        for s in stages:
            assert len(s.job_ids) == 1

    def test_worker_limit_larger_than_stage(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        stages = parallel_stages(graph, worker_limit=10)
        assert len(stages) == 3
        assert stages[0].job_ids == ["A"]
        assert set(stages[1].job_ids) == {"B", "C"}
        assert stages[2].job_ids == ["D"]

    def test_worker_limit_exact(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        stages = parallel_stages(graph, worker_limit=4)
        assert len(stages) == 1
        assert len(stages[0].job_ids) == 4

    def test_worker_limit_zero_unlimited(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        stages_unlimited = parallel_stages(graph, worker_limit=0)
        stages_default = parallel_stages(graph)
        assert len(stages_unlimited) == len(stages_default)

    def test_worker_limit_negative_raises(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        with pytest.raises(ValueError, match="non-negative"):
            parallel_stages(graph, worker_limit=-1)

    def test_stages_maintain_topo_order(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        stages = parallel_stages(graph)
        all_jobs = []
        for s in stages:
            all_jobs.extend(s.job_ids)
        # B before C in original SIMPLE_JOBS, both in stage 1
        b_idx = all_jobs.index("B")
        c_idx = all_jobs.index("C")
        assert b_idx < c_idx  # should preserve original order

    def test_cyclic_graph_raises_on_stages(self):
        graph = JobGraph.from_dict(CYCLIC_JOBS)
        with pytest.raises(ValidationError):
            parallel_stages(graph)


# ======================================================================
#  M4: Critical path
# ======================================================================


class TestCriticalPath:
    def test_simple_critical_path(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        # A(3) -> B(2) -> D(1)  =  6
        # A(3) -> C(4) -> D(1)  =  8  ← critical
        cp = critical_path(graph)
        assert cp.total_time == 3 + 4 + 1  # = 8
        # Path should be A -> C -> D
        assert cp.path == ["A", "C", "D"]

    def test_diamond_critical_path(self):
        graph = JobGraph.from_dict(DIAMOND_JOBS)
        # A(5) -> B(3) -> D(4)  =  12
        # A(5) -> C(2) -> D(4)  =  11
        cp = critical_path(graph)
        assert cp.total_time == 12
        assert cp.path == ["A", "B", "D"]

    def test_chain_critical_path(self):
        graph = JobGraph.from_dict(CHAIN_JOBS)
        cp = critical_path(graph)
        assert cp.total_time == 1 + 2 + 3 + 4
        assert cp.path == ["A", "B", "C", "D"]

    def test_single_job_critical_path(self):
        graph = JobGraph.from_dict(SINGLE_JOB)
        cp = critical_path(graph)
        assert cp.total_time == 10
        assert cp.path == ["only"]

    def test_empty_graph(self):
        graph = JobGraph.from_dict([])
        cp = critical_path(graph)
        assert cp.total_time == 0
        assert cp.path == []

    def test_all_independent(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        cp = critical_path(graph)
        assert cp.total_time == 4  # longest single job is D (duration 4)
        assert cp.path == ["D"]

    def test_complex_critical_path(self):
        graph = JobGraph.from_dict(COMPLEX_JOBS)
        # A(5) -> B(6) -> D(2) -> F(1) -> G(7) = 21
        # A(5) -> B(6) -> E(4) -> F(1) -> G(7) = 23 ← critical (B->E is longer than B->D)
        # A(5) -> C(3) -> D(2) -> F(1) -> G(7) = 18
        cp = critical_path(graph)
        assert cp.total_time == 5 + 6 + 4 + 1 + 7  # = 23
        assert cp.path == ["A", "B", "E", "F", "G"]

    def test_earliest_times(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        cp = critical_path(graph)
        # A: start 0, finish 3
        assert cp.earliest_start["A"] == 0
        assert cp.earliest_finish["A"] == 3
        # B: start 3, finish 5
        assert cp.earliest_start["B"] == 3
        assert cp.earliest_finish["B"] == 5
        # C: start 3, finish 7
        assert cp.earliest_start["C"] == 3
        assert cp.earliest_finish["C"] == 7
        # D: start 7, finish 8
        assert cp.earliest_start["D"] == 7
        assert cp.earliest_finish["D"] == 8

    def test_deep_fork_join(self):
        graph = JobGraph.from_dict(DEEP_FORK_JOIN)
        cp = critical_path(graph)
        # A(2) -> B(3) -> D(1) -> E(4) -> F(2) -> H(1) = 13
        # A(2) -> C(5) -> E(4) -> F(2) -> H(1)          = 14 ← critical
        # A(2) -> C(5) -> E(4) -> G(3) -> H(1)           = 15 ← CRITICAL
        assert cp.total_time == 2 + 5 + 4 + 3 + 1  # = 15
        assert cp.path == ["A", "C", "E", "G", "H"]

    def test_cyclic_graph_raises(self):
        graph = JobGraph.from_dict(CYCLIC_JOBS)
        with pytest.raises(ValidationError):
            critical_path(graph)


# ======================================================================
#  M5: Downstream failure propagation
# ======================================================================


class TestDownstreamJobs:
    def test_downstream_simple(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        # A -> B, A -> C -> D
        downstream = downstream_jobs(graph, "A")
        assert set(downstream) == {"B", "C", "D"}

    def test_downstream_middle(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        # B -> D
        downstream = downstream_jobs(graph, "B")
        assert downstream == ["D"]

    def test_downstream_leaf(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        downstream = downstream_jobs(graph, "D")
        assert downstream == []

    def test_downstream_independent(self):
        graph = JobGraph.from_dict(WIDE_JOBS)
        downstream = downstream_jobs(graph, "A")
        assert downstream == []  # No downstream

    def test_downstream_in_diamond(self):
        graph = JobGraph.from_dict(DIAMOND_JOBS)
        # A -> B, C -> D
        downstream = downstream_jobs(graph, "A")
        assert set(downstream) == {"B", "C", "D"}

        downstream = downstream_jobs(graph, "B")
        assert downstream == ["D"]

    def test_downstream_chain(self):
        graph = JobGraph.from_dict(CHAIN_JOBS)
        assert downstream_jobs(graph, "A") == ["B", "C", "D"]
        assert downstream_jobs(graph, "B") == ["C", "D"]
        assert downstream_jobs(graph, "C") == ["D"]
        assert downstream_jobs(graph, "D") == []

    def test_downstream_nonexistent_job_raises(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        with pytest.raises(ValidationError, match="not found"):
            downstream_jobs(graph, "NONEXISTENT")

    def test_downstream_complex(self):
        graph = JobGraph.from_dict(COMPLEX_JOBS)
        # A -> everything
        downstream = downstream_jobs(graph, "A")
        assert set(downstream) == {"B", "C", "D", "E", "F", "G"}

        # B -> D, E, F, G
        downstream = downstream_jobs(graph, "B")
        assert set(downstream) == {"D", "E", "F", "G"}

        # C -> D, F, G
        downstream = downstream_jobs(graph, "C")
        assert set(downstream) == {"D", "F", "G"}

        # G is sink
        assert downstream_jobs(graph, "G") == []


# ======================================================================
#  Scheduler integration tests
# ======================================================================


class TestSchedulerIntegration:
    def test_from_json_string(self):
        scheduler = Scheduler('[{"id":"A","duration":5,"dependsOn":[]}]')
        order = scheduler.execution_order()
        assert order == ["A"]

    def test_from_dict(self):
        scheduler = Scheduler(SIMPLE_JOBS)
        assert scheduler.execution_order() == ["A", "B", "C", "D"]

    def test_from_jobgraph(self):
        graph = JobGraph.from_dict(SIMPLE_JOBS)
        scheduler = Scheduler(graph)
        assert scheduler.execution_order() == ["A", "B", "C", "D"]

    def test_summary_valid(self):
        scheduler = Scheduler(SIMPLE_JOBS)
        summary = scheduler.summary()
        assert summary["valid"] is True
        assert summary["num_jobs"] == 4
        assert summary["total_time"] == 8
        assert summary["critical_path"] == ["A", "C", "D"]

    def test_summary_invalid_cycle(self):
        scheduler = Scheduler(CYCLIC_JOBS)
        summary = scheduler.summary()
        assert summary["valid"] is False
        assert "errors" in summary

    def test_round_trip(self):
        # Pipeline: validate → order → stages → critical-path
        scheduler = Scheduler(COMPLEX_JOBS)

        assert scheduler.validate().is_valid

        order = scheduler.execution_order()
        assert len(order) == len(COMPLEX_JOBS)

        stages = scheduler.parallel_stages()
        assert len(stages) > 0

        cp = scheduler.critical_path()
        assert cp.total_time == 23

        assert "G" not in scheduler.downstream_of("G")
        assert "F" in scheduler.downstream_of("E")


# ======================================================================
#  Edge cases
# ======================================================================


class TestEdgeCases:
    def test_job_with_no_dependsOn_field(self):
        """Default to empty list if dependsOn omitted."""
        job = Job.from_dict({"id": "A", "duration": 5})
        assert job.dependsOn == []

    def test_case_sensitive_ids(self):
        """Job IDs should be case-sensitive."""
        jobs = [
            {"id": "A", "duration": 1, "dependsOn": []},
            {"id": "a", "duration": 2, "dependsOn": ["A"]},
        ]
        graph = JobGraph.from_dict(jobs)
        assert graph.get_job("A") is not None
        assert graph.get_job("a") is not None
        assert topological_sort(graph)

    def test_ids_with_special_characters(self):
        jobs = [
            {"id": "job-1", "duration": 1, "dependsOn": []},
            {"id": "job_2", "duration": 2, "dependsOn": ["job-1"]},
            {"id": "job.3", "duration": 3, "dependsOn": ["job-1"]},
        ]
        graph = JobGraph.from_dict(jobs)
        order = topological_sort(graph)
        assert order[0] == "job-1"

    def test_long_durations(self):
        jobs = [
            {"id": "A", "duration": 10**9, "dependsOn": []},
            {"id": "B", "duration": 10**9, "dependsOn": ["A"]},
        ]
        graph = JobGraph.from_dict(jobs)
        cp = critical_path(graph)
        assert cp.total_time == 2 * 10**9

    def test_empty_dependsOn_explicit(self):
        job = Job.from_dict({"id": "A", "duration": 5, "dependsOn": []})
        assert job.dependsOn == []

    def test_invalid_json_whitespace(self):
        scheduler = Scheduler("  [  ]  ")
        assert scheduler.execution_order() == []

    def test_downstream_self_affects_none(self):
        """A job failing should not list itself as downstream."""
        jobs = [
            {"id": "A", "duration": 1, "dependsOn": []},
        ]
        graph = JobGraph.from_dict(jobs)
        assert downstream_jobs(graph, "A") == []

    def test_worker_limit_split_non_independent_stage(self):
        """When a level has more jobs than worker_limit, it splits."""
        jobs = [
            {"id": "A", "duration": 1, "dependsOn": []},
            {"id": "B", "duration": 2, "dependsOn": []},
            {"id": "C", "duration": 3, "dependsOn": []},
            {"id": "D", "duration": 4, "dependsOn": ["A", "B", "C"]},
        ]
        graph = JobGraph.from_dict(jobs)
        stages = parallel_stages(graph, worker_limit=2)
        # Stage 0: A, B, C — split into 2 stages of 2 + 1
        # Stage 1: D
        assert len(stages) == 3
        assert len(stages[0].job_ids) == 2
        assert len(stages[1].job_ids) == 1
        assert len(stages[2].job_ids) == 1

    def test_invalid_input_does_not_crash_scheduler(self):
        with pytest.raises((ValueError, TypeError, json.JSONDecodeError)):
            Scheduler("{{invalid json}}")

    def test_scheduler_from_empty_list(self):
        scheduler = Scheduler([])
        assert scheduler.execution_order() == []
        assert scheduler.critical_path().total_time == 0

    def test_all_durations_zero(self):
        jobs = [
            {"id": "A", "duration": 0, "dependsOn": []},
            {"id": "B", "duration": 0, "dependsOn": ["A"]},
            {"id": "C", "duration": 0, "dependsOn": []},
        ]
        graph = JobGraph.from_dict(jobs)
        cp = critical_path(graph)
        assert cp.total_time == 0
        assert cp.path == ["A"] or cp.path == ["C"]  # both are valid critical paths

    def test_self_referencing_diamond_no_cycle(self):
        """A -> B, A -> C, B -> D, C -> D (diamond is acyclic)."""
        jobs = [
            {"id": "A", "duration": 5, "dependsOn": []},
            {"id": "B", "duration": 3, "dependsOn": ["A"]},
            {"id": "C", "duration": 2, "dependsOn": ["A"]},
            {"id": "D", "duration": 4, "dependsOn": ["B", "C"]},
        ]
        graph = JobGraph.from_dict(jobs)
        assert find_cycle(graph) is None
        assert validate_graph(graph).is_valid
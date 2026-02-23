from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


ExecutionMode = Literal["self", "cluster"]


@dataclass(frozen=True)
class CampaignConfig:
    id: str
    results_dir: Path = Path("results")
    repeats: int = 1
    seed: int = 0
    profile: str = "small"


@dataclass(frozen=True)
class ExecutionConfig:
    mode: ExecutionMode = "self"
    # Per-point timeout to avoid hanging the whole run.
    point_timeout_s: float = 60.0
    # Best-effort flush/close timeout per endpoint.
    close_timeout_s: float = 5.0


@dataclass(frozen=True)
class ComparatorSpec:
    name: str
    type: str
    env: dict[str, str] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    matrix: dict[str, Any] = field(default_factory=dict)
    points: list[dict[str, Any]] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BenchConfig:
    campaign: CampaignConfig
    execution: ExecutionConfig
    comparators: list[ComparatorSpec]
    workloads: list[WorkloadSpec]


@dataclass(frozen=True)
class ManifestPoint:
    point_id: str
    comparator: str
    workload: str
    repeat: int
    params: dict[str, Any]

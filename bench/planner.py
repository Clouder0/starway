from __future__ import annotations

import hashlib
import itertools
import json
from typing import Any, Iterable

from .types import BenchConfig, ManifestPoint, WorkloadSpec


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return [value]


def _expand_workload_points(spec: WorkloadSpec) -> list[dict[str, Any]]:
    """Expand a workload into a list of concrete parameter dictionaries."""

    if spec.points:
        return [dict(p) for p in spec.points]

    if not spec.matrix:
        return [{}]

    keys = sorted(spec.matrix.keys())
    value_lists: list[list[Any]] = [_as_list(spec.matrix[k]) for k in keys]
    points: list[dict[str, Any]] = []
    for values in itertools.product(*value_lists):
        points.append({k: v for k, v in zip(keys, values, strict=True)})
    return points


def _stable_point_id(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def plan_manifest(config: BenchConfig) -> list[ManifestPoint]:
    """Expand config into an explicit, deterministic list of runs."""

    manifest: list[ManifestPoint] = []
    for comparator in config.comparators:
        for workload in config.workloads:
            for params in _expand_workload_points(workload):
                for repeat in range(1, config.campaign.repeats + 1):
                    payload = {
                        "campaign_id": config.campaign.id,
                        "comparator": comparator.name,
                        "workload": workload.name,
                        "repeat": repeat,
                        "params": params,
                    }
                    manifest.append(
                        ManifestPoint(
                            point_id=_stable_point_id(payload),
                            comparator=comparator.name,
                            workload=workload.name,
                            repeat=repeat,
                            params=dict(params),
                        )
                    )

    # Deterministic order helps with reproducibility and debugging.
    manifest.sort(
        key=lambda p: (
            p.comparator,
            p.workload,
            json.dumps(p.params, sort_keys=True),
            p.repeat,
        )
    )
    return manifest

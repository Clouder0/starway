from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .types import (
    BenchConfig,
    CampaignConfig,
    ComparatorSpec,
    ExecutionConfig,
    WorkloadSpec,
)


def _require_mapping(value: Any, *, ctx: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected mapping for {ctx}, got {type(value).__name__}")
    return value


def _require_list(value: Any, *, ctx: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list for {ctx}, got {type(value).__name__}")
    return value


def load_config(path: str | Path) -> BenchConfig:
    """Load benchmark config.

    Benchmark configs are TOML, parsed with stdlib `tomllib`.
    """

    path = Path(path)
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
    except Exception as exc:
        raise ValueError(f"Failed to parse TOML config ({path})") from exc
    raw = _require_mapping(raw, ctx=f"root config ({path})")

    campaign_raw = _require_mapping(raw.get("campaign"), ctx="campaign")
    campaign_id = campaign_raw.get("id")
    if not isinstance(campaign_id, str) or not campaign_id.strip():
        raise ValueError("campaign.id must be a non-empty string")
    results_dir = campaign_raw.get("results_dir", "results")
    if not isinstance(results_dir, str) or not results_dir.strip():
        raise ValueError("campaign.results_dir must be a string")
    repeats = int(campaign_raw.get("repeats", 1))
    seed = int(campaign_raw.get("seed", 0))
    profile = campaign_raw.get("profile", "small")
    if not isinstance(profile, str) or not profile.strip():
        raise ValueError("campaign.profile must be a string")
    campaign = CampaignConfig(
        id=campaign_id,
        results_dir=Path(results_dir),
        repeats=repeats,
        seed=seed,
        profile=profile,
    )

    execution_raw = _require_mapping(raw.get("execution", {}), ctx="execution")
    mode = execution_raw.get("mode", "self")
    if mode not in ("self", "cluster"):
        raise ValueError("execution.mode must be 'self' or 'cluster'")
    point_timeout_s = float(execution_raw.get("point_timeout_s", 60.0))
    if point_timeout_s <= 0:
        raise ValueError("execution.point_timeout_s must be > 0")
    close_timeout_s = float(execution_raw.get("close_timeout_s", 5.0))
    if close_timeout_s <= 0:
        raise ValueError("execution.close_timeout_s must be > 0")

    execution = ExecutionConfig(
        mode=mode, point_timeout_s=point_timeout_s, close_timeout_s=close_timeout_s
    )

    comparators_raw = _require_list(raw.get("comparators"), ctx="comparators")
    comparators: list[ComparatorSpec] = []
    for idx, item in enumerate(comparators_raw):
        item = _require_mapping(item, ctx=f"comparators[{idx}]")
        name = item.get("name")
        typ = item.get("type")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"comparators[{idx}].name must be a non-empty string")
        if not isinstance(typ, str) or not typ.strip():
            raise ValueError(f"comparators[{idx}].type must be a non-empty string")
        env = item.get("env", {})
        env = _require_mapping(env, ctx=f"comparators[{idx}].env")
        options = item.get("options", {})
        options = _require_mapping(options, ctx=f"comparators[{idx}].options")
        comparators.append(
            ComparatorSpec(
                name=name,
                type=typ,
                env={str(k): str(v) for k, v in env.items()},
                options=dict(options),
            )
        )
    if not comparators:
        raise ValueError("comparators must be a non-empty list")

    workloads_raw = _require_list(raw.get("workloads"), ctx="workloads")
    workloads: list[WorkloadSpec] = []
    for idx, item in enumerate(workloads_raw):
        item = _require_mapping(item, ctx=f"workloads[{idx}]")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"workloads[{idx}].name must be a non-empty string")
        matrix = item.get("matrix", {})
        matrix = _require_mapping(matrix, ctx=f"workloads[{idx}].matrix")
        points = item.get("points", [])
        points = _require_list(points, ctx=f"workloads[{idx}].points")
        options = item.get("options", {})
        options = _require_mapping(options, ctx=f"workloads[{idx}].options")
        workloads.append(
            WorkloadSpec(
                name=name,
                matrix=dict(matrix),
                points=[
                    _require_mapping(p, ctx=f"workloads[{idx}].points[*]")
                    for p in points
                ],
                options=dict(options),
            )
        )
    if not workloads:
        raise ValueError("workloads must be a non-empty list")

    return BenchConfig(
        campaign=campaign,
        execution=execution,
        comparators=comparators,
        workloads=workloads,
    )


def stable_config_hash(config: BenchConfig) -> str:
    """Stable hash of the effective config (for result metadata)."""

    payload = asdict(config)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

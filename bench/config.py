from __future__ import annotations

import hashlib
import ipaddress
import json
import tomllib
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .types import (
    BenchConfig,
    CampaignConfig,
    ClusterConfig,
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


def _require_non_empty_str(value: Any, *, ctx: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{ctx} must be a non-empty string")
    return str(value)


def _require_ipv4(value: Any, *, ctx: str) -> str:
    host = _require_non_empty_str(value, ctx=ctx)
    try:
        ipaddress.IPv4Address(host)
    except Exception as exc:
        raise ValueError(f"{ctx} must be an IPv4 address string") from exc
    return host


def _require_port(value: Any, *, ctx: str) -> int:
    try:
        port = int(value)
    except Exception as exc:
        raise ValueError(f"{ctx} must be an integer port") from exc
    if not (1 <= port <= 65535):
        raise ValueError(f"{ctx} must be in [1, 65535]")
    return int(port)


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

    topology = execution_raw.get("topology", "two_process")
    if not isinstance(topology, str) or not topology.strip():
        raise ValueError("execution.topology must be a non-empty string")
    if topology != "two_process":
        raise ValueError(
            "execution.topology must be 'two_process' (inproc topology was removed)"
        )

    cpu_affinity_raw = execution_raw.get("cpu_affinity", [])
    if cpu_affinity_raw is None:
        cpu_affinity_raw = []
    cpu_affinity_raw = _require_list(cpu_affinity_raw, ctx="execution.cpu_affinity")
    cpu_affinity: tuple[int, ...] = tuple(int(v) for v in cpu_affinity_raw)
    if any(v < 0 for v in cpu_affinity):
        raise ValueError("execution.cpu_affinity values must be >= 0")

    payload_verify = execution_raw.get("payload_verify", "none")
    if not isinstance(payload_verify, str) or not payload_verify.strip():
        raise ValueError("execution.payload_verify must be a non-empty string")
    if payload_verify not in ("none", "preflight", "full"):
        raise ValueError(
            "execution.payload_verify must be 'none', 'preflight', or 'full'"
        )

    point_timeout_s = float(execution_raw.get("point_timeout_s", 60.0))
    if point_timeout_s <= 0:
        raise ValueError("execution.point_timeout_s must be > 0")
    close_timeout_s = float(execution_raw.get("close_timeout_s", 5.0))
    if close_timeout_s <= 0:
        raise ValueError("execution.close_timeout_s must be > 0")

    execution = ExecutionConfig(
        mode=mode,
        topology=topology,
        cpu_affinity=cpu_affinity,
        payload_verify=payload_verify,
        point_timeout_s=point_timeout_s,
        close_timeout_s=close_timeout_s,
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

    cluster: ClusterConfig | None = None
    if execution.mode == "cluster":
        cluster_doc = raw.get("cluster")
        if cluster_doc is None:
            raise ValueError(
                "cluster.coordinator_bind_host is required when execution.mode='cluster'"
            )
        cluster_raw = _require_mapping(cluster_doc, ctx="cluster")

        coordinator_bind_host = _require_ipv4(
            cluster_raw.get("coordinator_bind_host"),
            ctx="cluster.coordinator_bind_host",
        )
        coordinator_port = _require_port(
            cluster_raw.get("coordinator_port"), ctx="cluster.coordinator_port"
        )
        server_agent_id = _require_non_empty_str(
            cluster_raw.get("server_agent_id"), ctx="cluster.server_agent_id"
        )
        client_agent_id = _require_non_empty_str(
            cluster_raw.get("client_agent_id"), ctx="cluster.client_agent_id"
        )

        port_range_min = _require_port(
            cluster_raw.get("port_range_min"), ctx="cluster.port_range_min"
        )
        port_range_max = _require_port(
            cluster_raw.get("port_range_max"), ctx="cluster.port_range_max"
        )
        if port_range_min > port_range_max:
            raise ValueError("cluster.port_range_min must be <= cluster.port_range_max")
        if (port_range_max - port_range_min + 1) < 2:
            raise ValueError(
                "cluster port range must contain at least 2 ports (for 2-socket workloads)"
            )

        agent_connect_timeout_s = float(
            cluster_raw.get("agent_connect_timeout_s", 15.0)
        )
        if agent_connect_timeout_s <= 0:
            raise ValueError("cluster.agent_connect_timeout_s must be > 0")
        agent_rpc_timeout_s = float(cluster_raw.get("agent_rpc_timeout_s", 10.0))
        if agent_rpc_timeout_s <= 0:
            raise ValueError("cluster.agent_rpc_timeout_s must be > 0")

        # Cluster mode is host-to-host; shm_py is single-host only.
        for c in comparators:
            if c.type == "shm_py":
                raise ValueError("cluster mode does not support shm_py")
            if c.type == "starway_ucx":
                raise ValueError(
                    "cluster mode does not support starway_ucx (worker-address); "
                    "use starway_tcp (sockaddr ip:port) instead"
                )

        cluster = ClusterConfig(
            coordinator_bind_host=coordinator_bind_host,
            coordinator_port=coordinator_port,
            server_agent_id=server_agent_id,
            client_agent_id=client_agent_id,
            port_range_min=port_range_min,
            port_range_max=port_range_max,
            agent_connect_timeout_s=agent_connect_timeout_s,
            agent_rpc_timeout_s=agent_rpc_timeout_s,
        )

    return BenchConfig(
        campaign=campaign,
        execution=execution,
        comparators=comparators,
        workloads=workloads,
        cluster=cluster,
    )


def stable_config_hash(config: BenchConfig) -> str:
    """Stable hash of the effective config (for result metadata)."""

    payload = asdict(config)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

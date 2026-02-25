from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


ExecutionMode = Literal["self", "cluster"]

# In-process topology (`inproc`) was removed; we only support a true 2-process
# client/server topology.
Topology = Literal["two_process"]
PayloadVerifyMode = Literal["none", "preflight", "full"]


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

    # Endpoints live in separate processes (client+server).
    topology: Topology = "two_process"

    # If provided (and topology == two_process), pin server/client processes.
    # Semantics: [cpu0] -> server+client; [cpu0, cpu1] -> server=cpu0, client=cpu1.
    cpu_affinity: tuple[int, ...] = ()

    # Optional data-plane verification strategy.
    payload_verify: PayloadVerifyMode = "none"

    # Per-point timeout to avoid hanging the whole run.
    point_timeout_s: float = 60.0
    # Best-effort flush/close timeout per endpoint.
    close_timeout_s: float = 5.0


@dataclass(frozen=True)
class ClusterConfig:
    # Coordinator listens here for agent control-plane connections.
    coordinator_bind_host: str
    coordinator_port: int

    # Logical IDs used to match agent daemons.
    server_agent_id: str
    client_agent_id: str

    # Allowed data-plane port range on the server host.
    port_range_min: int
    port_range_max: int

    # Control-plane timeouts.
    agent_connect_timeout_s: float = 15.0
    agent_rpc_timeout_s: float = 10.0


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
    cluster: ClusterConfig | None = None


@dataclass(frozen=True)
class ManifestPoint:
    point_id: str
    comparator: str
    workload: str
    repeat: int
    params: dict[str, Any]

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_topology_2proc_asyncio_tcp_payload_preflight_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="asyncio_tcp",
            comparator_env={},
            comparator_options={"tcp_nodelay": True},
            workload="control_rtt",
            params={"msg_bytes": 8, "warmup_iters": 3, "iters": 10, "seed": 1},
            cpu_affinity=(),
            payload_verify="preflight",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "rtt_p50_us" in metrics

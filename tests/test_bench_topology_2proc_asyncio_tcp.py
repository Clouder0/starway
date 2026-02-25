from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_topology_2proc_asyncio_tcp_control_rtt_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="asyncio_tcp",
            comparator_env={},
            comparator_options={"tcp_nodelay": True},
            workload="control_rtt",
            params={"msg_bytes": 8, "warmup_iters": 5, "iters": 20},
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=10.0,
    )

    metrics = res["metrics"]
    assert "rtt_p50_us" in metrics
    assert "rtt_p99_us" in metrics

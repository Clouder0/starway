from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_topology_2proc_starway_ucx_control_rtt_smoke() -> None:
    try:
        import starway  # noqa: F401
    except ImportError:
        pytest.skip("starway bindings not available")

    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="starway_ucx",
            comparator_env={"UCX_TLS": "tcp"},
            comparator_options={},
            workload="control_rtt",
            params={"msg_bytes": 8, "warmup_iters": 5, "iters": 20},
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "rtt_p50_us" in metrics
    assert "rtt_p99_us" in metrics


@pytest.mark.asyncio
async def test_topology_2proc_starway_ucx_bucket_duplex_step_smoke() -> None:
    try:
        import starway  # noqa: F401
    except ImportError:
        pytest.skip("starway bindings not available")

    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="starway_ucx",
            comparator_env={"UCX_TLS": "tcp"},
            comparator_options={},
            workload="bucket_duplex_step",
            params={
                "bucket_bytes": "4KiB",
                "buckets_per_step": 2,
                "steps": 2,
            },
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "aggregate_gbps" in metrics
    assert metrics["steps"] == 2.0

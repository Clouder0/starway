from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_topology_2proc_shm_py_control_rtt_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="shm_py",
            comparator_env={},
            comparator_options={
                "shm_target_bytes": 64 * 1024,
                "shm_min_slots": 4,
                "shm_max_slots": 4096,
                "spin_yield_every": 1,
            },
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

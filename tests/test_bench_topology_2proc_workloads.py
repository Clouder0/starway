from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_topology_2proc_asyncio_tcp_bulk_stream_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="asyncio_tcp",
            comparator_env={},
            comparator_options={"tcp_nodelay": True},
            workload="bulk_stream",
            params={
                "chunk_bytes": 256,
                "duration_s": 0.05,
                "inflight": 8,
                "ack_every_n": 4,
                "seed": 1,
            },
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "goodput_gbps" in metrics
    assert metrics["messages_sent"] >= 1


@pytest.mark.asyncio
async def test_topology_2proc_asyncio_tcp_bucket_duplex_step_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="asyncio_tcp",
            comparator_env={},
            comparator_options={"tcp_nodelay": True},
            workload="bucket_duplex_step",
            params={"bucket_bytes": 128, "buckets_per_step": 4, "steps": 3},
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "aggregate_gbps" in metrics
    assert metrics["steps"] == 3.0


@pytest.mark.asyncio
async def test_topology_2proc_asyncio_tcp_control_under_load_smoke() -> None:
    from bench.topology_2proc import run_two_process_point

    res = await asyncio.wait_for(
        run_two_process_point(
            comparator_type="asyncio_tcp",
            comparator_env={},
            comparator_options={"tcp_nodelay": True},
            workload="control_under_load",
            params={
                "control": {"msg_bytes": 8, "warmup_iters": 2, "iters": 20},
                "bulk": {
                    "chunk_bytes": 256,
                    "duration_s": 0.05,
                    "inflight": 8,
                    "ack_every_n": 4,
                    "seed": 1,
                },
            },
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "control_idle_p99_us" in metrics
    assert "control_loaded_p99_us" in metrics
    assert "p99_inflation_ratio" in metrics
    assert "bulk_goodput_gbps" in metrics


@pytest.mark.asyncio
async def test_topology_2proc_shm_py_control_under_load_smoke() -> None:
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
            workload="control_under_load",
            params={
                "control": {"msg_bytes": 8, "warmup_iters": 2, "iters": 20},
                "bulk": {
                    "chunk_bytes": 256,
                    "duration_s": 0.05,
                    "inflight": 8,
                    "ack_every_n": 4,
                    "seed": 1,
                },
            },
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "control_idle_p99_us" in metrics
    assert "control_loaded_p99_us" in metrics
    assert "p99_inflation_ratio" in metrics
    assert "bulk_goodput_gbps" in metrics


@pytest.mark.asyncio
async def test_topology_2proc_starway_ucx_control_under_load_smoke() -> None:
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
            workload="control_under_load",
            params={
                "control": {"msg_bytes": 8, "warmup_iters": 2, "iters": 20},
                "bulk": {
                    "chunk_bytes": 256,
                    "duration_s": 0.05,
                    "inflight": 8,
                    "ack_every_n": 4,
                    "seed": 1,
                },
            },
            cpu_affinity=(),
            payload_verify="none",
        ),
        timeout=20.0,
    )

    metrics = res["metrics"]
    assert "control_idle_p99_us" in metrics
    assert "control_loaded_p99_us" in metrics
    assert "p99_inflation_ratio" in metrics
    assert "bulk_goodput_gbps" in metrics

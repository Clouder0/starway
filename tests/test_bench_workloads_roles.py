from __future__ import annotations

import asyncio

import pytest

from bench.comparators import Pair
from bench.comparators.asyncio_tcp import create_self_pair


@pytest.mark.asyncio
async def test_control_rtt_roles_produce_latency_metrics() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    pair = Pair(a=a, b=b)
    try:
        from bench.workloads import control_rtt_client, control_rtt_server

        params = {"msg_bytes": 8, "warmup_iters": 5, "iters": 20}
        server_t = asyncio.create_task(control_rtt_server(pair.b, params))
        res = await control_rtt_client(pair.a, params)
        await server_t

        metrics = res["metrics"]
        assert "rtt_p50_us" in metrics
        assert "rtt_p99_us" in metrics
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_bulk_stream_roles_produce_goodput() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    pair = Pair(a=a, b=b)
    try:
        from bench.workloads import bulk_stream_client, bulk_stream_server

        params = {
            "chunk_bytes": 256,
            "duration_s": 0.05,
            "inflight": 8,
            "ack_every_n": 16,
            "seed": 1,
        }
        server_t = asyncio.create_task(bulk_stream_server(pair.b, params))
        res = await bulk_stream_client(pair.a, params)
        await server_t

        metrics = res["metrics"]
        assert "goodput_gbps" in metrics
        assert metrics["messages_sent"] >= 1
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_bucket_duplex_step_roles_produce_metrics() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    pair = Pair(a=a, b=b)
    try:
        from bench.workloads import bucket_duplex_step_client, bucket_duplex_step_server

        params = {"bucket_bytes": 256, "buckets_per_step": 4, "steps": 3}
        server_t = asyncio.create_task(bucket_duplex_step_server(pair.b, params))
        res = await bucket_duplex_step_client(pair.a, params)
        await server_t

        metrics = res["metrics"]
        assert "aggregate_gbps" in metrics
        assert metrics["steps"] == 3.0
    finally:
        await a.close()
        await b.close()

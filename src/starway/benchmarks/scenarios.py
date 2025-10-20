from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Protocol

import numpy as np

TAG_MASK: int = (1 << 64) - 1

CONTROL_TAG = 0x1AA0
READY_TAG = 0x1AA1
DONE_TAG = 0x1AA2

LARGE_DATA_TAG = 0x2B00
SMALL_DATA_TAG = 0x2B10
SMALL_ACK_TAG = 0x2B11  # reserved for future use
FLAG_PING_TAG = 0x2B20
FLAG_PONG_TAG = 0x2B21
STREAM_UP_TAG = 0x2B30
STREAM_DOWN_TAG = 0x2B31


class ClientRuntime(Protocol):
    client: Any
    tag_mask: int

    async def flush(self) -> None: ...


class ServerRuntime(Protocol):
    server: Any
    endpoint: Any
    tag_mask: int

    async def signal_ready(self) -> None: ...

    async def flush_endpoint(self) -> None: ...


@dataclass
class ScenarioResult:
    name: str
    metrics: Dict[str, float]
    samples: Dict[str, List[float]] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self, include_samples: bool = True) -> Dict[str, Any]:
        payload = {
            "name": self.name,
            "metrics": self.metrics,
            "config": self.config,
        }
        if include_samples:
            payload["samples"] = self.samples
        return payload


ClientRunner = Callable[[ClientRuntime, Mapping[str, Any]], Awaitable[ScenarioResult]]
ServerRunner = Callable[[ServerRuntime, Mapping[str, Any]], Awaitable[None]]


@dataclass
class ScenarioDefinition:
    name: str
    description: str
    defaults: Dict[str, Any]
    client_runner: ClientRunner
    server_runner: ServerRunner


def _merge_config(
    defaults: Mapping[str, Any], overrides: Mapping[str, Any]
) -> Dict[str, Any]:
    merged = dict(defaults)
    merged.update({k: v for k, v in overrides.items() if v is not None})
    return merged


async def _run_large_array_client(
    ctx: ClientRuntime, config: Mapping[str, Any]
) -> ScenarioResult:
    cfg = _merge_config(LARGE_ARRAY.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])
    total_iters = warmup + iterations

    payload = np.empty(message_bytes, dtype=np.uint8)
    payload.fill(0x5A)

    durations: list[float] = []
    per_iter_gbps: list[float] = []

    for idx in range(total_iters):
        start = time.perf_counter()
        await ctx.client.asend(payload, LARGE_DATA_TAG)
        await ctx.flush()
        elapsed = time.perf_counter() - start
        if idx >= warmup:
            durations.append(elapsed)
            if elapsed > 0:
                per_iter_gbps.append((message_bytes / elapsed) / 1e9)

    total_bytes = message_bytes * iterations
    total_time = sum(durations)
    avg_gbps = (total_bytes / total_time) / 1e9 if total_time > 0 else 0.0
    metrics = {
        "total_seconds": total_time,
        "avg_seconds_per_iter": (total_time / iterations) if iterations else 0.0,
        "avg_gbps": avg_gbps,
        "best_gbps": max(per_iter_gbps) if per_iter_gbps else 0.0,
        "worst_gbps": min(per_iter_gbps) if per_iter_gbps else 0.0,
    }

    return ScenarioResult(
        name="large-array",
        metrics=metrics,
        samples={"duration_seconds": durations, "per_iter_gbps": per_iter_gbps},
        config=dict(cfg),
    )


async def _run_large_array_server(
    ctx: ServerRuntime, config: Mapping[str, Any]
) -> None:
    cfg = _merge_config(LARGE_ARRAY.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])
    total_iters = warmup + iterations

    recv_buffer = np.empty(message_bytes, dtype=np.uint8)

    await ctx.signal_ready()
    for _ in range(total_iters):
        await ctx.server.arecv(recv_buffer, LARGE_DATA_TAG, ctx.tag_mask)
    await ctx.flush_endpoint()


async def _run_small_messages_client(
    ctx: ClientRuntime, config: Mapping[str, Any]
) -> ScenarioResult:
    cfg = _merge_config(SMALL_MESSAGES.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup_batches"])
    iterations = int(cfg["iterations"])
    concurrency = int(cfg["concurrency"])

    payloads = [
        np.full(message_bytes, fill_value=i % 251, dtype=np.uint8)
        for i in range(concurrency)
    ]

    durations: list[float] = []
    per_message_latency: list[float] = []

    for batch_idx in range(warmup + iterations):
        start = time.perf_counter()
        send_tasks = [ctx.client.asend(buf, SMALL_DATA_TAG) for buf in payloads]
        await asyncio.gather(*send_tasks)
        await ctx.flush()
        elapsed = time.perf_counter() - start
        if batch_idx >= warmup:
            durations.append(elapsed)
            if concurrency:
                per_message_latency.append(elapsed / concurrency)

    total_batches = iterations
    total_messages = total_batches * concurrency
    total_time = sum(durations)

    msg_per_sec = (total_messages / total_time) if total_time > 0 else 0.0
    bandwidth_gbps = (
        ((message_bytes * total_messages) / total_time) / 1e9 if total_time > 0 else 0.0
    )

    p50_us = p95_us = 0.0
    if per_message_latency:
        latencies_us = np.array(per_message_latency) * 1e6
        p50_us = float(np.percentile(latencies_us, 50))
        p95_us = float(np.percentile(latencies_us, 95))
    metrics = {
        "total_seconds": total_time,
        "messages_per_second": msg_per_sec,
        "bandwidth_gbps": bandwidth_gbps,
        "latency_p50_us": p50_us,
        "latency_p95_us": p95_us,
    }

    samples = {
        "batch_duration_seconds": durations,
        "avg_latency_seconds": per_message_latency,
    }

    return ScenarioResult(
        name="small-messages",
        metrics=metrics,
        samples=samples,
        config=dict(cfg),
    )


async def _run_small_messages_server(
    ctx: ServerRuntime, config: Mapping[str, Any]
) -> None:
    cfg = _merge_config(SMALL_MESSAGES.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup_batches"])
    iterations = int(cfg["iterations"])
    concurrency = int(cfg["concurrency"])

    buffers = [np.empty(message_bytes, dtype=np.uint8) for _ in range(concurrency)]

    await ctx.signal_ready()
    for _ in range(warmup + iterations):
        recv_tasks = [
            ctx.server.arecv(buf, SMALL_DATA_TAG, ctx.tag_mask) for buf in buffers
        ]
        await asyncio.gather(*recv_tasks)
    await ctx.flush_endpoint()


async def _run_pingpong_client(
    ctx: ClientRuntime, config: Mapping[str, Any]
) -> ScenarioResult:
    cfg = _merge_config(PINGPONG_FLAG.defaults, config)
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])

    send_buf = np.array([1], dtype=np.uint8)
    recv_buf = np.zeros(1, dtype=np.uint8)

    durations: list[float] = []

    # Warmup
    for _ in range(warmup):
        recv_future = ctx.client.arecv(recv_buf, FLAG_PONG_TAG, ctx.tag_mask)
        await ctx.client.asend(send_buf, FLAG_PING_TAG)
        await recv_future

    for _ in range(iterations):
        recv_future = ctx.client.arecv(recv_buf, FLAG_PONG_TAG, ctx.tag_mask)
        start = time.perf_counter()
        await ctx.client.asend(send_buf, FLAG_PING_TAG)
        await recv_future
        durations.append(time.perf_counter() - start)

    await ctx.flush()

    if durations:
        latencies_us = np.array(durations) * 1e6
        avg_rtt_us = float(np.mean(latencies_us))
        min_rtt_us = float(np.min(latencies_us))
        max_rtt_us = float(np.max(latencies_us))
        median_rtt_us = float(np.median(latencies_us))
    else:
        avg_rtt_us = min_rtt_us = max_rtt_us = median_rtt_us = 0.0

    metrics = {
        "avg_rtt_us": avg_rtt_us,
        "median_rtt_us": median_rtt_us,
        "min_rtt_us": min_rtt_us,
        "max_rtt_us": max_rtt_us,
        "avg_one_way_us": avg_rtt_us / 2.0,
    }

    return ScenarioResult(
        name="pingpong-flag",
        metrics=metrics,
        samples={"rtt_seconds": durations},
        config=dict(cfg),
    )


async def _run_pingpong_server(ctx: ServerRuntime, config: Mapping[str, Any]) -> None:
    cfg = _merge_config(PINGPONG_FLAG.defaults, config)
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])

    recv_buf = np.zeros(1, dtype=np.uint8)
    ack_buf = np.array([1], dtype=np.uint8)

    await ctx.signal_ready()
    for _ in range(warmup + iterations):
        await ctx.server.arecv(recv_buf, FLAG_PING_TAG, ctx.tag_mask)
        await ctx.server.asend(ctx.endpoint, ack_buf, FLAG_PONG_TAG)
    await ctx.flush_endpoint()


async def _run_streaming_duplex_client(
    ctx: ClientRuntime, config: Mapping[str, Any]
) -> ScenarioResult:
    cfg = _merge_config(STREAMING_DUPLEX.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])

    send_buf = np.full(message_bytes, fill_value=0x7B, dtype=np.uint8)
    recv_buf = np.empty(message_bytes, dtype=np.uint8)

    durations: list[float] = []

    for idx in range(warmup + iterations):
        recv_future = ctx.client.arecv(recv_buf, STREAM_DOWN_TAG, ctx.tag_mask)
        start = time.perf_counter()
        send_future = ctx.client.asend(send_buf, STREAM_UP_TAG)
        await asyncio.gather(send_future, recv_future)
        elapsed = time.perf_counter() - start
        if idx >= warmup:
            durations.append(elapsed)

    await ctx.flush()

    total_time = sum(durations)
    bytes_per_direction = message_bytes * iterations

    metrics = {
        "total_seconds": total_time,
        "avg_seconds_per_iter": (total_time / iterations) if iterations else 0.0,
        "client_to_server_gbps": (bytes_per_direction / total_time) / 1e9
        if total_time
        else 0.0,
        "server_to_client_gbps": (bytes_per_direction / total_time) / 1e9
        if total_time
        else 0.0,
        "aggregate_gbps": (2 * bytes_per_direction / total_time) / 1e9
        if total_time
        else 0.0,
    }

    return ScenarioResult(
        name="streaming-duplex",
        metrics=metrics,
        samples={"iteration_seconds": durations},
        config=dict(cfg),
    )


async def _run_streaming_duplex_server(
    ctx: ServerRuntime, config: Mapping[str, Any]
) -> None:
    cfg = _merge_config(STREAMING_DUPLEX.defaults, config)
    message_bytes = int(cfg["message_bytes"])
    warmup = int(cfg["warmup"])
    iterations = int(cfg["iterations"])

    send_buf = np.full(message_bytes, fill_value=0x3C, dtype=np.uint8)
    recv_buf = np.empty(message_bytes, dtype=np.uint8)

    await ctx.signal_ready()
    for _ in range(warmup + iterations):
        recv_future = ctx.server.arecv(recv_buf, STREAM_UP_TAG, ctx.tag_mask)
        send_future = ctx.server.asend(ctx.endpoint, send_buf, STREAM_DOWN_TAG)
        await asyncio.gather(recv_future, send_future)
    await ctx.flush_endpoint()


LARGE_ARRAY = ScenarioDefinition(
    name="large-array",
    description="Measure one-way bandwidth by transferring a single large buffer.",
    defaults={
        "message_bytes": 1 << 30,  # 1 GiB
        "warmup": 1,
        "iterations": 3,
    },
    client_runner=_run_large_array_client,
    server_runner=_run_large_array_server,
)

SMALL_MESSAGES = ScenarioDefinition(
    name="small-messages",
    description="Stress many small messages with configurable concurrency.",
    defaults={
        "message_bytes": 1024,
        "warmup_batches": 2,
        "iterations": 10,
        "concurrency": 64,
    },
    client_runner=_run_small_messages_client,
    server_runner=_run_small_messages_server,
)

PINGPONG_FLAG = ScenarioDefinition(
    name="pingpong-flag",
    description="Round-trip a single-byte control flag to capture latency.",
    defaults={
        "warmup": 100,
        "iterations": 1000,
    },
    client_runner=_run_pingpong_client,
    server_runner=_run_pingpong_server,
)

STREAMING_DUPLEX = ScenarioDefinition(
    name="streaming-duplex",
    description="Bidirectional medium-sized streaming in both directions.",
    defaults={
        "message_bytes": 4 * 1024 * 1024,
        "warmup": 8,
        "iterations": 64,
    },
    client_runner=_run_streaming_duplex_client,
    server_runner=_run_streaming_duplex_server,
)

SCENARIOS: Dict[str, ScenarioDefinition] = {
    LARGE_ARRAY.name: LARGE_ARRAY,
    SMALL_MESSAGES.name: SMALL_MESSAGES,
    PINGPONG_FLAG.name: PINGPONG_FLAG,
    STREAMING_DUPLEX.name: STREAMING_DUPLEX,
}

__all__ = [
    "SCENARIOS",
    "ScenarioDefinition",
    "ScenarioResult",
    "ClientRuntime",
    "ServerRuntime",
    "CONTROL_TAG",
    "READY_TAG",
    "DONE_TAG",
    "TAG_MASK",
]

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import numpy as np

from .comparators import Pair
from .util import parse_size


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    arr = np.asarray(values, dtype=np.float64)
    return float(np.percentile(arr, p))


def _latency_metrics_us(durations_s: list[float]) -> dict[str, float]:
    if not durations_s:
        return {
            "rtt_p50_us": 0.0,
            "rtt_p95_us": 0.0,
            "rtt_p99_us": 0.0,
            "rtt_p999_us": 0.0,
            "rtt_min_us": 0.0,
            "rtt_max_us": 0.0,
        }
    us = [d * 1e6 for d in durations_s]
    return {
        "rtt_p50_us": _pct(us, 50),
        "rtt_p95_us": _pct(us, 95),
        "rtt_p99_us": _pct(us, 99),
        "rtt_p999_us": _pct(us, 99.9),
        "rtt_min_us": float(min(us)),
        "rtt_max_us": float(max(us)),
    }


async def control_rtt(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
    """Ping-pong RTT on a single connection."""

    msg_bytes = parse_size(params.get("msg_bytes", 64))
    warmup_iters = int(params.get("warmup_iters", 100))
    iters = int(params.get("iters", 1000))

    ping_kind = int(params.get("ping_kind", 1))
    pong_kind = int(params.get("pong_kind", 2))

    send_buf = np.zeros(msg_bytes, dtype=np.uint8)
    recv_buf = np.empty(msg_bytes, dtype=np.uint8)

    async def side_b() -> None:
        # Respond to warmup + measured pings.
        expected = warmup_iters + iters
        for _ in range(expected):
            kind, seq, length = await pair.b.recv_any(recv_buf)
            if kind != ping_kind:
                raise RuntimeError(f"unexpected kind {kind} in control_rtt")
            await pair.b.send(pong_kind, seq, send_buf)

    responder = asyncio.create_task(side_b())
    durations: list[float] = []
    try:
        # Warmup
        for i in range(1, warmup_iters + 1):
            await pair.a.send(ping_kind, i, send_buf)
            kind, seq, _ = await pair.a.recv_any(recv_buf)
            if kind != pong_kind or seq != i:
                raise RuntimeError("bad pong")

        for i in range(warmup_iters + 1, warmup_iters + iters + 1):
            t0 = time.perf_counter()
            await pair.a.send(ping_kind, i, send_buf)
            kind, seq, _ = await pair.a.recv_any(recv_buf)
            t1 = time.perf_counter()
            if kind != pong_kind or seq != i:
                raise RuntimeError("bad pong")
            durations.append(t1 - t0)
    finally:
        # If the benchmark is cancelled (e.g. point-level timeout), make sure we
        # don't leak the responder task.
        if not responder.done():
            responder.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await responder

    metrics = _latency_metrics_us(durations)
    metrics["message_bytes"] = float(msg_bytes)
    metrics["iterations"] = float(iters)
    return {"metrics": metrics}


async def _one_way_stream(
    *,
    sender,
    receiver,
    data_kind: int,
    ack_kind: int,
    stop_kind: int,
    payload_pool: list[np.ndarray],
    payload_sizes: list[int],
    rng: np.random.Generator,
    duration_s: float,
    inflight: int,
    ack_every_n: int,
    min_bytes: int = 0,
) -> dict[str, Any]:
    """Delivery-complete one-way stream with cumulative ACK + STOP."""

    if inflight <= 0:
        raise ValueError("inflight must be > 0")
    if ack_every_n <= 0:
        raise ValueError("ack_every_n must be > 0")
    if duration_s <= 0:
        raise ValueError("duration_s must be > 0")

    warnings: list[str] = []

    # If ACK cadence is larger than the sender inflight window, the sender can
    # deadlock waiting for an ACK that can never be produced. Clamp to inflight.
    ack_every_n_effective = min(ack_every_n, inflight)
    if ack_every_n_effective != ack_every_n:
        warnings.append(
            "ack_every_n clamped: "
            f"requested={ack_every_n} effective={ack_every_n_effective} inflight={inflight}"
        )

    ack_buf = np.array([1], dtype=np.uint8)

    max_payload = max(payload_sizes) if payload_sizes else 0
    recv_buf = np.empty(max_payload, dtype=np.uint8)

    acked_seq = 0
    ack_event = asyncio.Event()
    stop_seq: int | None = None

    async def ack_receiver() -> None:
        nonlocal acked_seq
        tmp = np.empty(1, dtype=np.uint8)
        while True:
            kind, seq, _ = await sender.recv_any(tmp)
            if kind != ack_kind:
                raise RuntimeError(f"unexpected kind {kind} in ack_receiver")
            if seq > acked_seq:
                acked_seq = seq
                ack_event.set()
            if stop_seq is not None and acked_seq >= stop_seq:
                return

    bytes_received = 0
    messages_received = 0
    contig_received = 0
    out_of_order: set[int] = set()
    last_ack_sent = 0

    async def data_receiver() -> int:
        """Receive data until STOP is safe to ACK.

        We only ACK contiguous delivery progress. For delivery-complete semantics,
        we delay ACKing STOP until all seq < stop_seq have been received.
        """

        nonlocal bytes_received, messages_received, contig_received, last_ack_sent
        stop_seen: int | None = None

        while True:
            kind, seq, length = await receiver.recv_any(recv_buf)
            if kind == data_kind:
                bytes_received += int(length)
                messages_received += 1

                if seq == contig_received + 1:
                    contig_received = seq
                    # Drain any contiguous seqs we already received out-of-order.
                    while (contig_received + 1) in out_of_order:
                        out_of_order.remove(contig_received + 1)
                        contig_received += 1
                elif seq > contig_received + 1:
                    out_of_order.add(seq)

                # Throttled cumulative ACK: report contiguous delivery progress.
                if (
                    contig_received - last_ack_sent >= ack_every_n_effective
                    and contig_received
                ):
                    last_ack_sent = contig_received
                    await receiver.send(ack_kind, contig_received, ack_buf)

                # If we've already seen STOP, ACK it only once all data arrives.
                if stop_seen is not None:
                    stop_seq = int(stop_seen)
                    if contig_received >= stop_seq - 1:
                        await receiver.send(ack_kind, stop_seq, ack_buf)
                        return stop_seq

            elif kind == stop_kind:
                stop_seen = int(seq)
                if contig_received >= stop_seen - 1:
                    await receiver.send(ack_kind, stop_seen, ack_buf)
                    return stop_seen

            else:
                raise RuntimeError(f"unexpected kind {kind} in data_receiver")

    ack_task = asyncio.create_task(ack_receiver())
    recv_task = asyncio.create_task(data_receiver())

    bytes_sent = 0
    messages_sent = 0
    seq = 1
    # Only set after STOP is sent, so the ack_receiver can exit cleanly.
    # (Avoid cancelling recv tasks, which can leave UCX requests dangling.)
    t0 = time.perf_counter()
    deadline = t0 + duration_s

    try:
        while time.perf_counter() < deadline:
            # Sliding window: keep at most inflight unacked messages.
            while seq - acked_seq > inflight:
                ack_event.clear()
                await ack_event.wait()

            size = int(rng.choice(payload_sizes))
            buf = payload_pool[(seq - 1) % len(payload_pool)]
            view = buf[:size]
            await sender.send(data_kind, seq, view)
            bytes_sent += size
            messages_sent += 1
            seq += 1

        stop_seq = seq
        await sender.send(stop_kind, stop_seq, ack_buf)

        stop_seen = await recv_task
        if stop_seen != stop_seq:
            raise RuntimeError("receiver stop seq mismatch")

        # Wait for final ACK (delivery-complete).
        await ack_task
        t1 = time.perf_counter()

        # Invariants for "delivery-complete".
        if messages_received != messages_sent:
            raise RuntimeError(
                f"delivery incomplete: messages_sent={messages_sent} "
                f"messages_delivered={messages_received}"
            )
        if bytes_received != bytes_sent:
            raise RuntimeError(
                f"delivery incomplete: bytes_sent={bytes_sent} bytes_delivered={bytes_received}"
            )

        elapsed = t1 - t0
        goodput_gbps = (bytes_received * 8 / elapsed) / 1e9 if elapsed > 0 else 0.0

        out: dict[str, Any] = {
            "metrics": {
                "duration_s": float(duration_s),
                "elapsed_s": float(elapsed),
                "bytes_sent": float(bytes_sent),
                "bytes_delivered": float(bytes_received),
                "messages_sent": float(messages_sent),
                "messages_delivered": float(messages_received),
                "goodput_gbps": float(goodput_gbps),
                "ack_every_n_effective": float(ack_every_n_effective),
                "volume_ok": float(1.0 if bytes_received >= min_bytes else 0.0),
            }
        }
        if warnings:
            out["warnings"] = warnings
        return out
    finally:
        if not ack_task.done():
            ack_task.cancel()
        if not recv_task.done():
            recv_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ack_task
        with contextlib.suppress(asyncio.CancelledError):
            await recv_task


async def small_message_pressure(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
    msg_bytes = parse_size(params.get("msg_bytes", 1024))
    duration_s = float(params.get("duration_s", 2.0))
    inflight = int(params.get("inflight", 64))
    ack_every_n = int(params.get("ack_every_n", 256))

    rng = np.random.default_rng(int(params.get("seed", 0)))
    pool = [
        np.zeros(msg_bytes, dtype=np.uint8) for _ in range(max(1, min(inflight, 4)))
    ]

    return await _one_way_stream(
        sender=pair.a,
        receiver=pair.b,
        data_kind=1,
        ack_kind=2,
        stop_kind=3,
        payload_pool=pool,
        payload_sizes=[msg_bytes],
        rng=rng,
        duration_s=duration_s,
        inflight=inflight,
        ack_every_n=ack_every_n,
        min_bytes=parse_size(params.get("min_bytes", 0)),
    )


async def bulk_stream(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
    chunk_bytes = parse_size(params.get("chunk_bytes", "4MiB"))
    duration_s = float(params.get("duration_s", 2.0))
    inflight = int(params.get("inflight", 16))
    ack_every_n = int(params.get("ack_every_n", 32))

    rng = np.random.default_rng(int(params.get("seed", 0)))
    pool = [
        np.zeros(chunk_bytes, dtype=np.uint8) for _ in range(max(1, min(inflight, 4)))
    ]
    return await _one_way_stream(
        sender=pair.a,
        receiver=pair.b,
        data_kind=1,
        ack_kind=2,
        stop_kind=3,
        payload_pool=pool,
        payload_sizes=[chunk_bytes],
        rng=rng,
        duration_s=duration_s,
        inflight=inflight,
        ack_every_n=ack_every_n,
        min_bytes=parse_size(params.get("min_bytes", 0)),
    )


async def mixed_size(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
    sizes = [parse_size(v) for v in params.get("sizes", ["64B", "1KiB", "64KiB"])]
    duration_s = float(params.get("duration_s", 2.0))
    inflight = int(params.get("inflight", 64))
    ack_every_n = int(params.get("ack_every_n", 256))
    seed = int(params.get("seed", 0))

    rng = np.random.default_rng(seed)
    max_size = max(sizes)
    pool = [np.zeros(max_size, dtype=np.uint8) for _ in range(max(1, min(inflight, 4)))]

    # Use numpy choice over sizes directly (uniform by default).
    return await _one_way_stream(
        sender=pair.a,
        receiver=pair.b,
        data_kind=1,
        ack_kind=2,
        stop_kind=3,
        payload_pool=pool,
        payload_sizes=sizes,
        rng=rng,
        duration_s=duration_s,
        inflight=inflight,
        ack_every_n=ack_every_n,
        min_bytes=parse_size(params.get("min_bytes", 0)),
    )


async def bucket_duplex_step(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
    bucket_bytes = parse_size(params.get("bucket_bytes", "4MiB"))
    buckets_per_step = int(params.get("buckets_per_step", 8))
    steps = int(params.get("steps", 10))

    kind_bucket = int(params.get("bucket_kind", 1))

    send_a = np.zeros(bucket_bytes, dtype=np.uint8)
    send_b = np.zeros(bucket_bytes, dtype=np.uint8)
    recv_a = np.empty(bucket_bytes, dtype=np.uint8)
    recv_b = np.empty(bucket_bytes, dtype=np.uint8)

    step_times: list[float] = []

    async def run_step(peer_self, send_buf, recv_buf, start_seq: int) -> None:
        async def send_loop() -> None:
            for i in range(buckets_per_step):
                await peer_self.send(kind_bucket, start_seq + i, send_buf)

        async def recv_loop() -> None:
            for _ in range(buckets_per_step):
                kind, _, _ = await peer_self.recv_any(recv_buf)
                if kind != kind_bucket:
                    raise RuntimeError(f"unexpected kind {kind} in bucket_duplex_step")

        await asyncio.gather(send_loop(), recv_loop())

    seq_offset_b = 10_000_000
    for step in range(steps):
        start_a = step * buckets_per_step
        start_b = seq_offset_b + step * buckets_per_step
        t0 = time.perf_counter()
        await asyncio.gather(
            run_step(pair.a, send_a, recv_a, start_a),
            run_step(pair.b, send_b, recv_b, start_b),
        )
        t1 = time.perf_counter()
        step_times.append(t1 - t0)

    total_payload = bucket_bytes * buckets_per_step * steps
    total_bits = total_payload * 2 * 8  # duplex
    total_time = sum(step_times)
    aggregate_gbps = (total_bits / total_time) / 1e9 if total_time > 0 else 0.0
    metrics = {
        "steps": float(steps),
        "bucket_bytes": float(bucket_bytes),
        "buckets_per_step": float(buckets_per_step),
        "step_p50_ms": _pct([t * 1e3 for t in step_times], 50),
        "step_p95_ms": _pct([t * 1e3 for t in step_times], 95),
        "step_p99_ms": _pct([t * 1e3 for t in step_times], 99),
        "aggregate_gbps": float(aggregate_gbps),
    }
    return {"metrics": metrics}


async def control_under_load(
    *,
    data_pair: Pair,
    control_pair: Pair,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Measure control RTT while a bulk stream runs in parallel.

    MVP uses *two separate connections* (one bulk, one control) to avoid
    implementing a general-purpose demux layer in v1.
    """

    control_params = dict(params.get("control", {}))
    bulk_params = dict(params.get("bulk", {}))

    # Idle control baseline.
    idle = await control_rtt(control_pair, control_params)
    idle_p99 = float(idle["metrics"].get("rtt_p99_us", 0.0))

    # Structured concurrency: if the point is cancelled/times out while we're
    # measuring under load, ensure the bulk stream task is cancelled too.
    async with asyncio.TaskGroup() as tg:
        bulk_t = tg.create_task(bulk_stream(data_pair, bulk_params))
        loaded_t = tg.create_task(control_rtt(control_pair, control_params))

    loaded = loaded_t.result()
    bulk_res = bulk_t.result()

    loaded_p99 = float(loaded["metrics"].get("rtt_p99_us", 0.0))
    inflation = (loaded_p99 / idle_p99) if idle_p99 > 0 else 0.0

    metrics = {
        "control_idle_p99_us": idle_p99,
        "control_loaded_p99_us": loaded_p99,
        "p99_inflation_ratio": float(inflation),
        "bulk_goodput_gbps": float(bulk_res["metrics"].get("goodput_gbps", 0.0)),
    }

    warnings: list[str] = []
    for sub in (idle, loaded, bulk_res):
        warnings.extend(list(sub.get("warnings", [])))

    # De-dup while preserving order.
    seen: set[str] = set()
    warnings = [w for w in warnings if not (w in seen or seen.add(w))]

    out: dict[str, Any] = {
        "metrics": metrics,
        "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res},
    }
    if warnings:
        out["warnings"] = warnings
    return out


WORKLOAD_RUNNERS = {
    "control_rtt": control_rtt,
    "small_message_pressure": small_message_pressure,
    "bulk_stream": bulk_stream,
    "mixed_size": mixed_size,
    "bucket_duplex_step": bucket_duplex_step,
    # control_under_load handled specially (needs two pairs)
}

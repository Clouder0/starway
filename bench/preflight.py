from __future__ import annotations

from collections.abc import Iterable

import numpy as np

from .comparators import Pair, Peer
from .kinds import KIND_PREFLIGHT


def _make_payload_array(size: int, *, seed: int) -> np.ndarray:
    if size < 0:
        raise ValueError("size must be >= 0")
    if size == 0:
        return np.empty(0, dtype=np.uint8)

    # Deterministic, non-trivial payload pattern to catch corruption.
    rng = np.random.default_rng(int(seed))
    return rng.integers(0, 256, size=size, dtype=np.uint8)


def _make_payload(size: int, *, seed: int) -> bytes:
    return _make_payload_array(size, seed=seed).tobytes()


async def preflight_verify_peer_server(
    peer: Peer, *, sizes: Iterable[int], seed: int = 0
) -> None:
    """Server-side preflight verification (expects client-driven echo pattern)."""

    sizes_list = [int(s) for s in sizes]
    if any(s < 0 for s in sizes_list):
        raise ValueError("sizes must be >= 0")

    max_size = max(sizes_list, default=0)
    recv_buf = np.empty(max_size, dtype=np.uint8)

    seq = 1
    for size in sizes_list:
        expected = _make_payload_array(size, seed=seed ^ (seq * 0x9E3779B1))
        kind, got_seq, length = await peer.recv_any(recv_buf)
        if kind != KIND_PREFLIGHT or got_seq != seq or length != size:
            raise RuntimeError(
                "preflight receive mismatch "
                f"(client->server): kind={kind} seq={got_seq} length={length} expected={size}"
            )
        if not np.array_equal(recv_buf[:length], expected):
            raise RuntimeError("payload mismatch (client->server)")

        # Echo back exactly what we received.
        await peer.send(KIND_PREFLIGHT, seq, recv_buf[:length])
        seq += 1


async def preflight_verify_peer_client(
    peer: Peer, *, sizes: Iterable[int], seed: int = 0
) -> None:
    """Client-side preflight verification (sends payloads and verifies echo)."""

    sizes_list = [int(s) for s in sizes]
    if any(s < 0 for s in sizes_list):
        raise ValueError("sizes must be >= 0")

    max_size = max(sizes_list, default=0)
    recv_buf = np.empty(max_size, dtype=np.uint8)

    seq = 1
    for size in sizes_list:
        payload = _make_payload_array(size, seed=seed ^ (seq * 0x9E3779B1))
        await peer.send(KIND_PREFLIGHT, seq, payload)
        kind, got_seq, length = await peer.recv_any(recv_buf)
        if kind != KIND_PREFLIGHT or got_seq != seq or length != size:
            raise RuntimeError(
                "preflight receive mismatch "
                f"(echo): kind={kind} seq={got_seq} length={length} expected={size}"
            )
        if not np.array_equal(recv_buf[:length], payload):
            raise RuntimeError("payload mismatch (echo)")

        seq += 1


async def preflight_verify_pair(
    pair: Pair, *, sizes: Iterable[int], seed: int = 0
) -> None:
    """Exchange a small set of messages and verify payload integrity.

    This is intended to run *outside* the timed measurement region to keep
    performance numbers fair, while still catching obvious comparator bugs.
    """

    sizes_list = [int(s) for s in sizes]
    if any(s < 0 for s in sizes_list):
        raise ValueError("sizes must be >= 0")

    max_size = max(sizes_list, default=0)
    recv_buf = bytearray(max_size)

    seq = 1
    for size in sizes_list:
        # A -> B
        payload = _make_payload(size, seed=seed ^ (seq * 0x9E3779B1))
        await pair.a.send(KIND_PREFLIGHT, seq, payload)
        kind, got_seq, length = await pair.b.recv_any(recv_buf)
        if kind != KIND_PREFLIGHT or got_seq != seq or length != size:
            raise RuntimeError(
                "preflight receive mismatch "
                f"(a->b): kind={kind} seq={got_seq} length={length} expected={size}"
            )
        got = bytes(recv_buf[:length])
        if got != payload:
            raise RuntimeError("payload mismatch (a->b)")

        seq += 1

        # B -> A
        payload = _make_payload(size, seed=seed ^ (seq * 0x9E3779B1))
        await pair.b.send(KIND_PREFLIGHT, seq, payload)
        kind, got_seq, length = await pair.a.recv_any(recv_buf)
        if kind != KIND_PREFLIGHT or got_seq != seq or length != size:
            raise RuntimeError(
                "preflight receive mismatch "
                f"(b->a): kind={kind} seq={got_seq} length={length} expected={size}"
            )
        got = bytes(recv_buf[:length])
        if got != payload:
            raise RuntimeError("payload mismatch (b->a)")

        seq += 1


__all__ = [
    "preflight_verify_pair",
    "preflight_verify_peer_client",
    "preflight_verify_peer_server",
]

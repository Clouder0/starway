from __future__ import annotations

import asyncio

import pytest

from bench.comparators.asyncio_tcp import create_self_pair


@pytest.mark.asyncio
async def test_barrier_completes_and_does_not_leak_messages() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    try:
        from bench.barrier import barrier

        # Both sides must be able to rendezvous.
        await asyncio.gather(barrier(a), barrier(b))

        # After the barrier, normal messages should flow; the barrier message
        # itself must have been consumed by barrier().
        payload = b"hello"
        await a.send(1, 123, payload)
        buf = bytearray(64)
        kind, seq, length = await b.recv_any(buf)
        assert (kind, seq, length) == (1, 123, len(payload))
        assert bytes(buf[:length]) == payload
    finally:
        await a.close()
        await b.close()

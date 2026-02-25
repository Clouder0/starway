from __future__ import annotations

import pytest

from bench.comparators import Pair
from bench.comparators.asyncio_tcp import create_self_pair


@pytest.mark.asyncio
async def test_preflight_verify_pair_passes_for_asyncio_tcp() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    pair = Pair(a=a, b=b)
    try:
        from bench.preflight import preflight_verify_pair

        await preflight_verify_pair(pair, sizes=[0, 1, 17, 128], seed=123)
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_preflight_verify_pair_detects_corruption() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})

    class CorruptingPeer:
        def __init__(self, inner):
            self._inner = inner

        async def send(self, kind: int, seq: int, payload):
            data = bytes(payload)
            if data:
                # Flip one byte deterministically.
                data = bytes([data[0] ^ 0xFF]) + data[1:]
            await self._inner.send(kind, seq, data)

        async def recv_any(self, buffer):
            return await self._inner.recv_any(buffer)

        async def close(self):
            return await self._inner.close()

    pair = Pair(a=CorruptingPeer(a), b=b)
    try:
        from bench.preflight import preflight_verify_pair

        with pytest.raises(RuntimeError, match="payload mismatch"):
            await preflight_verify_pair(pair, sizes=[32], seed=1)
    finally:
        await a.close()
        await b.close()

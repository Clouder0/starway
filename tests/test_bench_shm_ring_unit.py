from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_shm_ring_roundtrip_ordered_delivery() -> None:
    from bench.shm_ring import ShmRingSpec, attach_ring, create_ring

    spec, writer = create_ring(slots=8, slot_payload_bytes=64)
    reader = attach_ring(spec)
    try:

        async def do_write() -> None:
            for i in range(20):
                payload = bytes([i & 0xFF]) * 17
                await writer.write(kind=7, seq=i, payload=payload)
                await asyncio.sleep(0)

        async def do_read() -> None:
            buf = bytearray(64)
            for i in range(20):
                kind, seq, length = await reader.read_into(buf)
                assert (kind, seq, length) == (7, i, 17)
                assert bytes(buf[:length]) == (bytes([i & 0xFF]) * 17)

        await asyncio.gather(do_write(), do_read())
    finally:
        await writer.aclose()
        await reader.aclose()


@pytest.mark.asyncio
async def test_shm_ring_blocks_when_full_and_resumes() -> None:
    from bench.shm_ring import attach_ring, create_ring

    spec, writer = create_ring(slots=1, slot_payload_bytes=8, spin_yield_every=1)
    reader = attach_ring(spec, spin_yield_every=1)
    try:
        await writer.write(kind=1, seq=1, payload=b"a")

        blocked = asyncio.create_task(writer.write(kind=1, seq=2, payload=b"b"))
        await asyncio.sleep(0)
        assert not blocked.done()

        buf = bytearray(8)
        kind, seq, length = await reader.read_into(buf)
        assert (kind, seq, length) == (1, 1, 1)

        await asyncio.wait_for(blocked, timeout=0.2)

        kind, seq, length = await reader.read_into(buf)
        assert (kind, seq, length) == (1, 2, 1)
    finally:
        blocked.cancel()
        await writer.aclose()
        await reader.aclose()

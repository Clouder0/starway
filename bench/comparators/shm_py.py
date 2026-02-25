from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from bench.shm_ring import ShmRing, ShmRingSpec, attach_ring, create_ring
from bench.util import parse_size


def _int_bytes(value: Any) -> int:
    # Accept ints (already-bytes) and human-readable size strings.
    if isinstance(value, str):
        return parse_size(value)
    return int(value)


def _ring_stride(slot_payload_bytes: int) -> int:
    # Must match bench.shm_ring layout.
    header = 16
    cacheline = 64
    n = header + int(slot_payload_bytes)
    return ((n + cacheline - 1) // cacheline) * cacheline


def _choose_slots(
    *,
    slot_payload_bytes: int,
    target_bytes: int,
    min_slots: int,
    max_slots: int,
) -> int:
    stride = _ring_stride(slot_payload_bytes)
    slots = int(target_bytes) // int(stride) if stride > 0 else 0
    slots = max(int(min_slots), slots)
    slots = min(int(max_slots), slots)
    return int(slots)


@dataclass
class ShmPeer:
    _send_ring: ShmRing
    _recv_ring: ShmRing

    async def send(self, kind: int, seq: int, payload: Any) -> None:
        await self._send_ring.write(kind=int(kind), seq=int(seq), payload=payload)

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        return await self._recv_ring.read_into(buffer)

    async def close(self) -> None:
        await asyncio.gather(self._send_ring.aclose(), self._recv_ring.aclose())


async def create_self_pair(
    options: dict[str, Any] | None = None,
) -> tuple[ShmPeer, ShmPeer]:
    """Create an in-process shared-memory Peer pair.

    This is primarily used for unit/smoke tests; the two-process runner will
    construct shared memory segments in the parent and attach in child workers.
    """

    options = options or {}
    max_payload = _int_bytes(options.get("max_payload_bytes", 0))
    if max_payload <= 0:
        raise ValueError("shm_py requires options.max_payload_bytes > 0")

    target_bytes = _int_bytes(options.get("shm_target_bytes", 64 * 1024 * 1024))
    min_slots = int(options.get("shm_min_slots", 4))
    max_slots = int(options.get("shm_max_slots", 65536))
    spin_yield_every = int(options.get("spin_yield_every", 1024))

    slots = _choose_slots(
        slot_payload_bytes=max_payload,
        target_bytes=target_bytes,
        min_slots=min_slots,
        max_slots=max_slots,
    )

    spec_ab, ring_ab_owner = create_ring(
        slots=slots,
        slot_payload_bytes=max_payload,
        spin_yield_every=spin_yield_every,
    )
    spec_ba, ring_ba_owner = create_ring(
        slots=slots,
        slot_payload_bytes=max_payload,
        spin_yield_every=spin_yield_every,
    )

    # Attach additional handles so each peer has independent close semantics.
    ring_ab_attach = attach_ring(spec_ab, spin_yield_every=spin_yield_every)
    ring_ba_attach = attach_ring(spec_ba, spin_yield_every=spin_yield_every)

    peer_a = ShmPeer(_send_ring=ring_ab_owner, _recv_ring=ring_ba_attach)
    peer_b = ShmPeer(_send_ring=ring_ba_owner, _recv_ring=ring_ab_attach)
    return peer_a, peer_b


__all__ = [
    "ShmPeer",
    "create_self_pair",
    "ShmRingSpec",
]

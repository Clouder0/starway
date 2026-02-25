from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import Any

import numpy as np


# Reuse the bench protocol header shape: kind:uint8, seq:uint64, length:uint32.
_HEADER = struct.Struct("<B3xQI")
_HEADER_SIZE = _HEADER.size  # 16

_CACHELINE_BYTES = 64
_WRITE_COUNTER_OFF = 0
_READ_COUNTER_OFF = _CACHELINE_BYTES
_DATA_OFF = _CACHELINE_BYTES * 2


@dataclass(frozen=True)
class ShmRingSpec:
    name: str
    shm_size: int
    slots: int
    slot_payload_bytes: int
    slot_stride: int


def _align_up(n: int, align: int) -> int:
    if align <= 0:
        raise ValueError("align must be > 0")
    return ((n + align - 1) // align) * align


def _slot_stride(slot_payload_bytes: int) -> int:
    return _align_up(_HEADER_SIZE + int(slot_payload_bytes), _CACHELINE_BYTES)


class ShmRing:
    def __init__(
        self,
        shm: shared_memory.SharedMemory,
        spec: ShmRingSpec,
        *,
        owner: bool,
        spin_yield_every: int,
    ) -> None:
        self._shm = shm
        self.spec = spec
        self._owner = bool(owner)
        self._spin_yield_every = int(spin_yield_every)
        if self._spin_yield_every <= 0:
            raise ValueError("spin_yield_every must be > 0")

        # Two independent cache lines to reduce false sharing.
        self._write_ctr = np.ndarray(
            (1,), dtype=np.uint64, buffer=shm.buf, offset=_WRITE_COUNTER_OFF
        )
        self._read_ctr = np.ndarray(
            (1,), dtype=np.uint64, buffer=shm.buf, offset=_READ_COUNTER_OFF
        )
        self._buf = shm.buf

    def _slot_start(self, index: int) -> int:
        return _DATA_OFF + (index * self.spec.slot_stride)

    async def _spin_yield(self, spins: int) -> None:
        if spins % self._spin_yield_every == 0:
            await asyncio.sleep(0)

    async def write(self, *, kind: int, seq: int, payload: Any) -> None:
        kind_i = int(kind)
        seq_i = int(seq)
        payload_mv = memoryview(payload).cast("B")
        length = int(len(payload_mv))
        if length > self.spec.slot_payload_bytes:
            raise ValueError(
                f"payload too large: {length} > slot_payload_bytes={self.spec.slot_payload_bytes}"
            )

        slots = int(self.spec.slots)
        write_idx = int(self._write_ctr[0])

        spins = 0
        while write_idx - int(self._read_ctr[0]) >= slots:
            spins += 1
            await self._spin_yield(spins)

        slot = self._slot_start(write_idx % slots)
        _HEADER.pack_into(self._buf, slot, kind_i & 0xFF, seq_i, length)
        payload_off = slot + _HEADER_SIZE
        if length:
            self._buf[payload_off : payload_off + length] = payload_mv

        # Commit: reader treats write_ctr as the publish point.
        self._write_ctr[0] = np.uint64(write_idx + 1)

    async def read_into(self, buffer: Any) -> tuple[int, int, int]:
        buffer_mv = memoryview(buffer).cast("B")
        slots = int(self.spec.slots)
        read_idx = int(self._read_ctr[0])

        spins = 0
        while int(self._write_ctr[0]) == read_idx:
            spins += 1
            await self._spin_yield(spins)

        slot = self._slot_start(read_idx % slots)
        kind, seq, length = _HEADER.unpack_from(self._buf, slot)
        length_i = int(length)
        if length_i > len(buffer_mv):
            raise ValueError(
                f"buffer too small: need {length_i}, have {len(buffer_mv)}"
            )

        payload_off = slot + _HEADER_SIZE
        if length_i:
            buffer_mv[:length_i] = self._buf[payload_off : payload_off + length_i]

        self._read_ctr[0] = np.uint64(read_idx + 1)
        return int(kind), int(seq), length_i

    async def aclose(self) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._shm.close()
        finally:
            if self._owner:
                try:
                    self._shm.unlink()
                except FileNotFoundError:
                    pass


def create_ring(
    *,
    slots: int,
    slot_payload_bytes: int,
    spin_yield_every: int = 1024,
) -> tuple[ShmRingSpec, ShmRing]:
    slots_i = int(slots)
    if slots_i <= 0:
        raise ValueError("slots must be > 0")
    payload_i = int(slot_payload_bytes)
    if payload_i < 0:
        raise ValueError("slot_payload_bytes must be >= 0")

    stride = _slot_stride(payload_i)
    size = _DATA_OFF + (slots_i * stride)
    shm = shared_memory.SharedMemory(create=True, size=size)
    spec = ShmRingSpec(
        name=shm.name,
        shm_size=size,
        slots=slots_i,
        slot_payload_bytes=payload_i,
        slot_stride=stride,
    )
    ring = ShmRing(shm, spec, owner=True, spin_yield_every=spin_yield_every)

    # SharedMemory isn't guaranteed to be zeroed; initialize counters explicitly.
    ring._write_ctr[0] = np.uint64(0)
    ring._read_ctr[0] = np.uint64(0)
    return spec, ring


def attach_ring(spec: ShmRingSpec, *, spin_yield_every: int = 1024) -> ShmRing:
    shm = shared_memory.SharedMemory(name=spec.name, create=False)
    # The attach side never unlinks; creator is responsible for lifecycle.
    return ShmRing(shm, spec, owner=False, spin_yield_every=spin_yield_every)


__all__ = ["ShmRing", "ShmRingSpec", "create_ring", "attach_ring"]

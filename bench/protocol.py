from __future__ import annotations

import struct


# Network framing for the asyncio TCP comparator.
#
# Header layout (big-endian):
# - kind:   uint8
# - pad:    3 bytes
# - seq:    uint64
# - length: uint32
_HEADER_STRUCT = struct.Struct("!B3xQI")
HEADER_SIZE = _HEADER_STRUCT.size  # 16 bytes


def pack_header(kind: int, seq: int, length: int) -> bytes:
    if not (0 <= kind <= 255):
        raise ValueError("kind must fit in uint8")
    if not (0 <= seq < 2**64):
        raise ValueError("seq must fit in uint64")
    if not (0 <= length < 2**32):
        raise ValueError("length must fit in uint32")
    return _HEADER_STRUCT.pack(kind, seq, length)


def unpack_header(data: bytes) -> tuple[int, int, int]:
    kind, seq, length = _HEADER_STRUCT.unpack(data)
    return int(kind), int(seq), int(length)

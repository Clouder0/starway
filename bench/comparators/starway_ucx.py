from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from typing import Any

import numpy as np


TAG_KIND_SHIFT = 56
TAG_SEQ_MASK = (1 << TAG_KIND_SHIFT) - 1


def encode_tag(kind: int, seq: int) -> int:
    if not (0 <= kind <= 255):
        raise ValueError("kind must fit in uint8")
    if not (0 <= seq <= TAG_SEQ_MASK):
        raise ValueError("seq too large for tag encoding")
    return (kind << TAG_KIND_SHIFT) | seq


def decode_tag(tag: int) -> tuple[int, int]:
    kind = (tag >> TAG_KIND_SHIFT) & 0xFF
    seq = tag & TAG_SEQ_MASK
    return int(kind), int(seq)


def _apply_env(env: dict[str, str]) -> None:
    for key, value in env.items():
        existing = os.environ.get(key)
        if existing is None:
            os.environ[key] = value
        elif existing != value:
            raise RuntimeError(
                f"Comparator env conflict for {key}: '{existing}' != '{value}'. "
                "Starway/UCX env must be consistent within one process."
            )


def _as_uint8_ndarray(payload: Any) -> np.ndarray:
    """Coerce payloads into the ndarray[uint8] Starway bindings require."""

    if isinstance(payload, np.ndarray):
        arr = payload
        if arr.dtype != np.uint8:
            arr = arr.astype(np.uint8, copy=False)
        if arr.ndim != 1:
            arr = arr.reshape(-1)
        if not arr.flags.c_contiguous:
            arr = np.ascontiguousarray(arr)
        return arr

    if isinstance(payload, (bytes, bytearray, memoryview)):
        arr = np.frombuffer(payload, dtype=np.uint8)
        # frombuffer(bytes) produces a read-only view; Starway only reads.
        if not arr.flags.writeable:
            arr = arr.copy()
        return arr

    arr = np.asarray(payload, dtype=np.uint8)
    if arr.ndim != 1:
        arr = arr.reshape(-1)
    if not arr.flags.c_contiguous:
        arr = np.ascontiguousarray(arr)
    return arr


@dataclass
class StarwayClientPeer:
    client: Any

    async def send(self, kind: int, seq: int, payload: Any) -> None:
        tag = encode_tag(kind, seq)
        await self.client.asend(_as_uint8_ndarray(payload), tag)

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        sender_tag, length = await self.client.arecv(buffer, 0, 0)
        kind, seq = decode_tag(sender_tag)
        return kind, seq, int(length)

    async def close(self) -> None:
        await self.client.aclose()


@dataclass
class StarwayServerPeer:
    server: Any
    endpoint: Any

    async def send(self, kind: int, seq: int, payload: Any) -> None:
        tag = encode_tag(kind, seq)
        await self.server.asend(self.endpoint, _as_uint8_ndarray(payload), tag)

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        sender_tag, length = await self.server.arecv(buffer, 0, 0)
        kind, seq = decode_tag(sender_tag)
        return kind, seq, int(length)

    async def close(self) -> None:
        await self.server.aclose()


async def create_self_pair(
    env: dict[str, str] | None = None, options: dict[str, Any] | None = None
) -> tuple[StarwayClientPeer, StarwayServerPeer]:
    """Create a local Starway UCX pair via worker-address connect.

    This connection mode supports `UCX_TLS=sm` because it does not rely on TCP
    sockets for connection establishment.
    """

    env = env or {}
    options = options or {}
    _apply_env(env)

    # Import after env is applied (UCX reads env during init).
    import starway  # type: ignore

    server = starway.Server()
    client = starway.Client()

    try:
        worker_addr = server.listen_address()
        await client.aconnect_address(worker_addr)

        # Wait for endpoint discovery.
        deadline = asyncio.get_running_loop().time() + float(
            options.get("accept_timeout_s", 2.0)
        )
        endpoint = None
        while asyncio.get_running_loop().time() < deadline:
            clients = server.list_clients()
            if clients:
                endpoint = next(iter(clients))
                break
            await asyncio.sleep(0.01)

        if endpoint is None:
            raise TimeoutError("Timed out waiting for Starway server to accept client")

        peer_a = StarwayClientPeer(client=client)
        peer_b = StarwayServerPeer(server=server, endpoint=endpoint)
        return peer_a, peer_b
    except BaseException:
        # Best-effort cleanup for a benchmark harness, especially important when
        # points are cancelled/time out mid-setup.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(client.aclose(), timeout=1.0)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server.aclose(), timeout=1.0)
        raise


__all__ = [
    "StarwayClientPeer",
    "StarwayServerPeer",
    "create_self_pair",
    "encode_tag",
    "decode_tag",
]

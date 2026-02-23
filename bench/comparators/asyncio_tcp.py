from __future__ import annotations

import asyncio
import contextlib
import socket
from dataclasses import dataclass
from typing import Any

from ..protocol import HEADER_SIZE, pack_header, unpack_header


async def _recv_exactly_into(
    loop: asyncio.AbstractEventLoop, sock: socket.socket, buf: memoryview
) -> None:
    offset = 0
    while offset < len(buf):
        n = await loop.sock_recv_into(sock, buf[offset:])
        if n == 0:
            raise ConnectionError("socket closed")
        offset += n


@dataclass
class AsyncioTcpPeer:
    sock: socket.socket
    loop: asyncio.AbstractEventLoop
    # If payload <= threshold, coalesce header+payload into one send.
    coalesce_send_bytes: int = 4096

    async def send(self, kind: int, seq: int, payload: Any) -> None:
        payload_view = memoryview(payload)
        header = pack_header(kind, seq, len(payload_view))

        if (
            payload_view
            and self.coalesce_send_bytes > 0
            and len(payload_view) <= self.coalesce_send_bytes
        ):
            # One syscall for the common "small payload" case.
            await self.loop.sock_sendall(self.sock, header + payload_view.tobytes())
            return

        await self.loop.sock_sendall(self.sock, header)
        if payload_view:
            await self.loop.sock_sendall(self.sock, payload_view)

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        buffer_view = memoryview(buffer)
        header_buf = bytearray(HEADER_SIZE)
        await _recv_exactly_into(self.loop, self.sock, memoryview(header_buf))
        kind, seq, length = unpack_header(bytes(header_buf))
        if length:
            if length > len(buffer_view):
                raise ValueError(
                    f"recv buffer too small: need {length} bytes, have {len(buffer_view)}"
                )
            await _recv_exactly_into(self.loop, self.sock, buffer_view[:length])
        return kind, seq, length

    async def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            # Best-effort close for a benchmark harness.
            pass


async def create_self_pair(
    options: dict[str, Any] | None = None,
) -> tuple[AsyncioTcpPeer, AsyncioTcpPeer]:
    """Create a connected TCP pair (A <-> B) on loopback."""

    options = options or {}
    tcp_nodelay = bool(options.get("tcp_nodelay", True))
    coalesce_send_bytes = int(options.get("coalesce_send_bytes", 4096))
    if coalesce_send_bytes < 0:
        coalesce_send_bytes = 0

    loop = asyncio.get_running_loop()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    host, port = listener.getsockname()[:2]

    async def accept_one() -> socket.socket:
        conn, _ = await loop.sock_accept(listener)
        conn.setblocking(False)
        if tcp_nodelay:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return conn

    accept_task: asyncio.Task[socket.socket] = asyncio.create_task(accept_one())
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setblocking(False)
    if tcp_nodelay:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    server_conn: socket.socket | None = None
    try:
        await loop.sock_connect(client, (host, port))
        server_conn = await accept_task
        listener.close()

        peer_a = AsyncioTcpPeer(
            sock=client, loop=loop, coalesce_send_bytes=coalesce_send_bytes
        )
        peer_b = AsyncioTcpPeer(
            sock=server_conn, loop=loop, coalesce_send_bytes=coalesce_send_bytes
        )
        return peer_a, peer_b
    except BaseException:
        # Best-effort cleanup for a benchmark harness, especially important when
        # points are cancelled/time out mid-setup.
        if not accept_task.done():
            accept_task.cancel()

        with contextlib.suppress(Exception):
            listener.close()

        # If accept already succeeded but we never awaited it, close the
        # accepted socket too.
        if server_conn is None and accept_task.done() and not accept_task.cancelled():
            with contextlib.suppress(Exception):
                server_conn = accept_task.result()

        with contextlib.suppress(Exception):
            client.close()
        if server_conn is not None:
            with contextlib.suppress(Exception):
                server_conn.close()

        with contextlib.suppress(asyncio.CancelledError, Exception):
            await accept_task
        raise

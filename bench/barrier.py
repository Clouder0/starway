from __future__ import annotations

from .comparators import Peer
from .kinds import KIND_BARRIER


async def barrier(peer: Peer, *, seq: int = 0) -> None:
    """Rendezvous: returns only after the peer sees the matching barrier message."""

    # Both sides send, then both sides receive.
    await peer.send(KIND_BARRIER, int(seq), b"")

    tmp = bytearray(0)
    kind, got_seq, _ = await peer.recv_any(tmp)
    if kind != KIND_BARRIER or got_seq != int(seq):
        raise RuntimeError(
            f"unexpected message during barrier: kind={kind} seq={got_seq}"
        )


__all__ = ["barrier"]

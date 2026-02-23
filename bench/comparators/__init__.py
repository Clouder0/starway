from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class Peer(Protocol):
    async def send(self, kind: int, seq: int, payload: Any) -> None: ...

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class Pair:
    a: Peer
    b: Peer


__all__ = ["Peer", "Pair"]

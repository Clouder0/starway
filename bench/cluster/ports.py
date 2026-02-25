from __future__ import annotations


class PortAllocator:
    """Deterministic port allocator for a fixed inclusive range.

    This does not probe the OS for availability. Callers should handle bind errors.
    """

    def __init__(self, *, port_min: int, port_max: int) -> None:
        port_min = int(port_min)
        port_max = int(port_max)
        if not (1 <= port_min <= 65535):
            raise ValueError("port_min must be in [1, 65535]")
        if not (1 <= port_max <= 65535):
            raise ValueError("port_max must be in [1, 65535]")
        if port_min > port_max:
            raise ValueError("port_min must be <= port_max")
        self._min = port_min
        self._max = port_max
        self._next = port_min

    def allocate(self, *, count: int = 1) -> list[int]:
        count = int(count)
        if count <= 0:
            raise ValueError("count must be > 0")
        if (self._max - self._min + 1) < count:
            raise ValueError("port range too small for requested count")

        out: list[int] = []
        for _ in range(count):
            out.append(int(self._next))
            self._next += 1
            if self._next > self._max:
                self._next = self._min
        return out

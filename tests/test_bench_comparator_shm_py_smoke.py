from __future__ import annotations

import numpy as np
import pytest


@pytest.mark.asyncio
async def test_shm_py_comparator_smoke() -> None:
    from bench.comparators.shm_py import create_self_pair

    a, b = await create_self_pair(
        {
            "max_payload_bytes": 1024,
            "shm_target_bytes": 1024 * 1024,
            "spin_yield_every": 1,
        }
    )
    try:
        payload = np.arange(16, dtype=np.uint8)
        buf = np.empty(1024, dtype=np.uint8)

        await a.send(3, 7, payload)
        kind, seq, length = await b.recv_any(buf)
        assert (kind, seq, length) == (3, 7, len(payload))
        np.testing.assert_array_equal(buf[:length], payload)

        await b.send(4, 9, payload)
        kind, seq, length = await a.recv_any(buf)
        assert (kind, seq, length) == (4, 9, len(payload))
        np.testing.assert_array_equal(buf[:length], payload)
    finally:
        await a.close()
        await b.close()

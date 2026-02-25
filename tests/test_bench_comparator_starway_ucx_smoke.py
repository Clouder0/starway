from __future__ import annotations

import numpy as np
import pytest


@pytest.mark.asyncio
async def test_starway_ucx_worker_address_comparator_smoke(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Ensure UCX transport selection is set before Starway import.
    monkeypatch.setenv("UCX_TLS", "tcp")

    try:
        from bench.comparators.starway_ucx import create_self_pair
    except Exception as exc:
        pytest.fail(f"Failed to import comparator module: {exc}")

    try:
        a, b = await create_self_pair(env={"UCX_TLS": "tcp"}, options={})
    except ImportError as exc:
        pytest.skip(f"Starway not importable in this environment: {exc}")

    try:
        payload = np.arange(16, dtype=np.uint8)
        buf = np.empty_like(payload)

        await a.send(1, 123, payload)
        kind, seq, length = await b.recv_any(buf)
        assert (kind, seq, length) == (1, 123, len(payload))
        np.testing.assert_array_equal(buf[:length], payload)

        await b.send(2, 456, payload)
        kind, seq, length = await a.recv_any(buf)
        assert (kind, seq, length) == (2, 456, len(payload))
        np.testing.assert_array_equal(buf[:length], payload)
    finally:
        await a.close()
        await b.close()

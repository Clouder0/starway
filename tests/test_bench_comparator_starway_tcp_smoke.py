from __future__ import annotations

import numpy as np
import pytest


@pytest.mark.asyncio
async def test_starway_tcp_comparator_accepts_bytes_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Ensure UCX transport selection is set before Starway import.
    monkeypatch.setenv("UCX_TLS", "tcp")

    try:
        from bench.comparators.starway_tcp import create_self_pair
    except Exception as exc:
        pytest.fail(f"Failed to import comparator module: {exc}")

    try:
        a, b = await create_self_pair(env={"UCX_TLS": "tcp"}, options={})
    except ImportError as exc:
        pytest.skip(f"Starway not importable in this environment: {exc}")

    try:
        # Empty payload (used by barrier/control messages).
        buf = np.empty(1, dtype=np.uint8)
        await a.send(1, 123, b"")
        kind, seq, length = await b.recv_any(buf)
        assert (kind, seq, length) == (1, 123, 0)

        # Non-empty bytes payload.
        payload = b"hello"
        buf2 = np.empty(len(payload), dtype=np.uint8)
        await b.send(2, 456, payload)
        kind, seq, length = await a.recv_any(buf2)
        assert (kind, seq, length) == (2, 456, len(payload))
        np.testing.assert_array_equal(
            buf2[:length], np.frombuffer(payload, dtype=np.uint8)
        )
    finally:
        await a.close()
        await b.close()

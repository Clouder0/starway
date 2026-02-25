from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_bulk_stream_wrapper_does_not_cancel_server_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: avoid cancelling server tasks in success path.

    Starway/UCX can crash/noise if tasks doing recv work are cancelled while
    operations are still in-flight. The wrapper should await server completion
    when the client returns cleanly.
    """

    import bench.workloads as workloads
    from bench.comparators import Pair

    server_started = asyncio.Event()
    server_finished = asyncio.Event()
    server_cancelled = False

    async def fake_server(_peer, _params):
        nonlocal server_cancelled
        server_started.set()
        try:
            await asyncio.sleep(0.05)
            server_finished.set()
        except asyncio.CancelledError:
            server_cancelled = True
            raise

    async def fake_client(_peer, _params):
        await server_started.wait()
        return {"metrics": {"ok": 1.0}}

    monkeypatch.setattr(workloads, "bulk_stream_server", fake_server)
    monkeypatch.setattr(workloads, "bulk_stream_client", fake_client)

    pair = Pair(a=object(), b=object())  # type: ignore[arg-type]
    result = await workloads.bulk_stream(pair, params={})
    assert result["metrics"]["ok"] == 1.0
    assert server_finished.is_set()
    assert not server_cancelled

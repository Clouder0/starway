from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

import bench.run as bench_run
import bench.workloads as workloads
from bench.comparators import Pair
from bench.comparators.asyncio_tcp import create_self_pair
from bench.comparators.asyncio_tcp import (
    create_self_pair as create_asyncio_tcp_self_pair,
)
from bench.run import run_campaign
from bench.workloads import small_message_pressure


@pytest.mark.asyncio
async def test_small_message_pressure_emits_ack_clamp_warning() -> None:
    a, b = await create_self_pair({"tcp_nodelay": True})
    pair = Pair(a=a, b=b)
    try:
        res = await small_message_pressure(
            pair,
            {
                "msg_bytes": 64,
                "duration_s": 0.05,
                "inflight": 4,
                "ack_every_n": 128,
                "seed": 1,
            },
        )
    finally:
        await a.close()
        await b.close()

    assert "warnings" in res
    assert any("ack_every_n clamped" in w for w in res["warnings"])


@pytest.mark.asyncio
async def test_run_campaign_times_out_a_point_and_continues(tmp_path: Path) -> None:
    results_dir = tmp_path / "results"
    cfg_path = tmp_path / "cfg.toml"

    cfg_text = f"""
[campaign]
id = "timeout_campaign"
results_dir = "{results_dir.as_posix()}"
repeats = 1
seed = 0

[execution]
mode = "self"
point_timeout_s = 0.2
close_timeout_s = 0.2

[[comparators]]
name = "asyncio_tcp"
type = "asyncio_tcp"

[[workloads]]
name = "hung"

[[workloads]]
name = "control_rtt"

[[workloads.points]]
msg_bytes = 1
warmup_iters = 1
iters = 1
""".lstrip()
    cfg_path.write_text(cfg_text, encoding="utf-8")

    async def hung_runner(pair: Pair, params: dict) -> dict:
        _ = pair, params
        await asyncio.sleep(1.0)
        return {"metrics": {}}

    bench_run.WORKLOAD_RUNNERS["hung"] = hung_runner
    try:
        rc = await run_campaign(cfg_path, dry_run=False)
        assert rc == 0
    finally:
        bench_run.WORKLOAD_RUNNERS.pop("hung", None)

    campaign_dir = results_dir / "timeout_campaign"
    run_dirs = [p for p in campaign_dir.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1

    out_dir = run_dirs[0]
    point_files = sorted((out_dir / "points").glob("*.json"))
    assert len(point_files) == 2

    statuses = [
        json.loads(p.read_text(encoding="utf-8"))["status"] for p in point_files
    ]
    assert "ok" in statuses
    assert "failed" in statuses


class _BlockingPeer:
    async def send(self, kind: int, seq: int, payload: Any) -> None:
        _ = kind, seq, payload
        # Yield control so the callee can schedule its responder task.
        await asyncio.sleep(0)

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        _ = buffer
        # Block forever unless cancelled.
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        return


class _NoopPeer:
    async def send(self, kind: int, seq: int, payload: Any) -> None:
        _ = kind, seq, payload

    async def recv_any(self, buffer: Any) -> tuple[int, int, int]:
        _ = buffer
        raise AssertionError("recv_any should not be called in this test")

    async def close(self) -> None:
        return


@pytest.mark.asyncio
async def test_control_rtt_cancel_does_not_leak_responder_task() -> None:
    pair = Pair(a=_BlockingPeer(), b=_BlockingPeer())

    before = set(asyncio.all_tasks())
    task = asyncio.create_task(
        workloads.control_rtt(pair, {"warmup_iters": 1, "iters": 1})
    )
    await asyncio.sleep(0)  # let responder task start
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)

    after = set(asyncio.all_tasks())
    leaked_side_b: list[asyncio.Task] = []
    for t in after - before:
        if t.done():
            continue
        coro = t.get_coro()
        name = getattr(getattr(coro, "cr_code", None), "co_name", "")
        if name == "side_b":
            leaked_side_b.append(t)

    # Cleanup even when failing, so we don't poison the rest of the suite.
    for t in leaked_side_b:
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    assert not leaked_side_b


@pytest.mark.asyncio
async def test_control_under_load_cancels_bulk_stream_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cancelled_by_control = asyncio.Event()
    bulk_task_ref: dict[str, asyncio.Task | None] = {"task": None}
    classify_as_control_cancel = {"value": True}

    control_calls = 0

    async def fake_bulk_stream(pair: Pair, params: dict) -> dict[str, Any]:
        _ = pair, params
        bulk_task_ref["task"] = asyncio.current_task()  # type: ignore[assignment]
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if classify_as_control_cancel["value"]:
                cancelled_by_control.set()
            raise

        return {"metrics": {}}

    async def fake_control_rtt(pair: Pair, params: dict[str, Any]) -> dict[str, Any]:
        nonlocal control_calls
        _ = pair, params
        control_calls += 1
        if control_calls == 1:
            return {"metrics": {"rtt_p99_us": 1.0}}

        # Second call represents the under-load measurement; block until cancelled.
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(workloads, "bulk_stream", fake_bulk_stream)
    monkeypatch.setattr(workloads, "control_rtt", fake_control_rtt)

    pair = Pair(a=_NoopPeer(), b=_NoopPeer())
    task = asyncio.create_task(
        workloads.control_under_load(data_pair=pair, control_pair=pair, params={})
    )
    await started.wait()

    try:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)

        # The bulk task should be cancelled by control_under_load cleanup.
        await asyncio.wait_for(cancelled_by_control.wait(), timeout=0.2)
    finally:
        # Cleanup in case of regression: cancel leaked bulk task without masking
        # the assertion (we flip the classifier before canceling).
        bulk_task = bulk_task_ref["task"]
        if bulk_task is not None and not bulk_task.done():
            classify_as_control_cancel["value"] = False
            bulk_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await bulk_task


@pytest.mark.asyncio
async def test_control_under_load_bubbles_bulk_warnings(tmp_path: Path) -> None:
    results_dir = tmp_path / "results"
    cfg_path = tmp_path / "cfg.toml"

    cfg_text = f"""
[campaign]
id = "warn_campaign"
results_dir = "{results_dir.as_posix()}"
repeats = 1
seed = 0

[execution]
mode = "self"
point_timeout_s = 2.0
close_timeout_s = 0.2

[[comparators]]
name = "asyncio_tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_under_load"

[[workloads.points]]
control = {{ msg_bytes = 1, warmup_iters = 1, iters = 1 }}
bulk = {{ chunk_bytes = "1B", duration_s = 0.05, inflight = 4, ack_every_n = 128 }}
""".lstrip()
    cfg_path.write_text(cfg_text, encoding="utf-8")

    rc = await run_campaign(cfg_path, dry_run=False)
    assert rc == 0

    campaign_dir = results_dir / "warn_campaign"
    run_dirs = [p for p in campaign_dir.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1

    point_files = sorted((run_dirs[0] / "points").glob("*.json"))
    assert len(point_files) == 1
    record = json.loads(point_files[0].read_text(encoding="utf-8"))
    assert record["status"] == "ok"
    assert any("ack_every_n clamped" in w for w in record.get("warnings", []))


@pytest.mark.asyncio
async def test_asyncio_tcp_create_self_pair_cancellation_cleans_accept_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    connect_started = asyncio.Event()

    orig_sock_connect = loop.sock_connect

    async def blocked_sock_connect(sock, addr):
        connect_started.set()
        # Block forever (cancel will interrupt this await).
        await asyncio.Event().wait()
        return await orig_sock_connect(sock, addr)

    monkeypatch.setattr(loop, "sock_connect", blocked_sock_connect)

    before = set(asyncio.all_tasks())
    task = asyncio.create_task(create_asyncio_tcp_self_pair({"tcp_nodelay": True}))
    await connect_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)

    after = set(asyncio.all_tasks())
    new_tasks = [t for t in (after - before) if not t.done()]
    accept_tasks: list[asyncio.Task] = []
    for t in new_tasks:
        coro = t.get_coro()
        name = getattr(getattr(coro, "cr_code", None), "co_name", "")
        if name == "accept_one":
            accept_tasks.append(t)

    leaked_accept_tasks = [t for t in accept_tasks if not t.done()]
    try:
        assert not leaked_accept_tasks
    finally:
        # Cleanup even when failing so the suite can continue.
        for t in leaked_accept_tasks:
            t.cancel()
            with pytest.raises(asyncio.CancelledError):
                await t

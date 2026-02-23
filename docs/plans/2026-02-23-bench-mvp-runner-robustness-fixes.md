# Bench MVP Runner Robustness Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the config-driven `bench/` self-runner resilient to timeouts/cancellation by preventing leaked background tasks/resources, and improve artifact hygiene/visibility (warnings + gitignore).

**Architecture:** Use Python 3.11 structured concurrency (`asyncio.TaskGroup`) where we intentionally run concurrent subtasks (e.g. `control_under_load`). For other background tasks, ensure explicit cancellation + best-effort awaits in `finally` blocks. Keep comparator setup (`create_self_pair`) exception/cancellation-safe with `try/except BaseException` cleanup.

**Tech Stack:** Python 3.11, `asyncio`, `pytest` + `pytest-asyncio`, stdlib `socket`.

---

### Task 1: Fix `control_rtt` responder task leaks on cancel/timeout

**Files:**
- Modify: `bench/workloads.py:42`
- Test: `tests/test_bench_timeouts.py`

**Step 1: Write the failing test**

Add a test that cancels `control_rtt()` mid-flight and asserts it doesn't leave an extra pending task behind.

```python
import asyncio

import pytest

import bench.workloads as workloads
from bench.comparators import Pair


class _BlockingPeer:
    async def send(self, kind: int, seq: int, payload):
        _ = kind, seq, payload
        await asyncio.sleep(0)

    async def recv_any(self, buffer):
        _ = buffer
        # Block forever unless cancelled.
        await asyncio.Event().wait()

    async def close(self) -> None:
        return


@pytest.mark.asyncio
async def test_control_rtt_cancel_does_not_leak_responder_task() -> None:
    pair = Pair(a=_BlockingPeer(), b=_BlockingPeer())

    before = set(asyncio.all_tasks())
    task = asyncio.create_task(workloads.control_rtt(pair, {"warmup_iters": 1, "iters": 1}))
    await asyncio.sleep(0)  # let responder task start
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    after = set(asyncio.all_tasks())
    leaked_side_b = []
    for t in (after - before):
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
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: FAIL because a responder task remains pending after cancellation.

**Step 3: Write minimal implementation**

In `bench/workloads.py` update `control_rtt()` to cancel the responder task in `finally` and best-effort await it.

```python
    responder = asyncio.create_task(side_b())
    durations: list[float] = []
    try:
        ...
    finally:
        if not responder.done():
            responder.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await responder
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: PASS.

**Step 5: Commit**

```bash
git add bench/workloads.py tests/test_bench_timeouts.py
git commit -m "fix(bench): cancel control_rtt responder on cancellation"
```

---

### Task 2: Make `control_under_load` structured-cancellation safe

**Files:**
- Modify: `bench/workloads.py:415`
- Test: `tests/test_bench_timeouts.py`

**Step 1: Write the failing test**

Add a unit test that cancels `control_under_load()` after the bulk task starts and asserts the bulk task is cancelled by `control_under_load` (not by the test cleanup).

```python
import asyncio

import pytest

import bench.workloads as workloads
from bench.comparators import Pair


class _NoopPeer:
    async def send(self, kind: int, seq: int, payload):
        _ = kind, seq, payload

    async def recv_any(self, buffer):
        raise AssertionError("recv_any should not be called in this test")

    async def close(self) -> None:
        return


@pytest.mark.asyncio
async def test_control_under_load_cancels_bulk_stream_when_cancelled(monkeypatch) -> None:
    started = asyncio.Event()
    cancelled_by_control = asyncio.Event()
    bulk_task_ref: dict[str, asyncio.Task | None] = {"task": None}
    classify_as_control_cancel = {"value": True}

    async def fake_bulk_stream(pair: Pair, params: dict):
        _ = pair, params
        bulk_task_ref["task"] = asyncio.current_task()  # type: ignore[assignment]
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if classify_as_control_cancel["value"]:
                cancelled_by_control.set()
            raise

    async def fake_control_rtt(pair: Pair, params: dict):
        _ = pair, params
        return {"metrics": {"rtt_p99_us": 1.0}}

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
            await task
        await asyncio.wait_for(cancelled_by_control.wait(), timeout=0.5)
    finally:
        # Cleanup in case of regression: cancel leaked bulk task without masking
        # the assertion (we flip the classifier before canceling).
        bulk_task = bulk_task_ref["task"]
        if bulk_task is not None and not bulk_task.done():
            classify_as_control_cancel["value"] = False
            bulk_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await bulk_task
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: FAIL (the bulk task is still running, so `cancelled_by_control` never fires).

**Step 3: Write minimal implementation**

Change `bench/workloads.py` `control_under_load()` to use `asyncio.TaskGroup` for the concurrent bulk/control segment.

```python
    async with asyncio.TaskGroup() as tg:
        bulk_t = tg.create_task(bulk_stream(data_pair, bulk_params))
        loaded_t = tg.create_task(control_rtt(control_pair, control_params))
    loaded = loaded_t.result()
    bulk_res = bulk_t.result()
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: PASS.

**Step 5: Commit**

```bash
git commit -m "fix(bench): prevent leaked bulk task under cancellation"
```

---

### Task 3: Bubble subresult warnings to top-level for `control_under_load`

**Files:**
- Modify: `bench/workloads.py:415`
- Test: `tests/test_bench_timeouts.py`

**Step 1: Write the failing integration test**

Run a tiny `control_under_load` point via `run_campaign()` where bulk stream triggers the `ack_every_n clamped` warning, and assert the point JSON has top-level `warnings`.

```python
import json
from pathlib import Path

import pytest

from bench.run import run_campaign


@pytest.mark.asyncio
async def test_control_under_load_bubbles_bulk_warnings(tmp_path: Path) -> None:
    results_dir = tmp_path / "results"
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(
        f"""
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
""".lstrip(),
        encoding="utf-8",
    )

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
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: FAIL because `control_under_load()` currently returns no top-level `warnings`.

**Step 3: Write minimal implementation**

In `bench/workloads.py`, after computing `bulk_res`, collect warnings from subresults and attach them to the top-level result.

```python
    warnings: list[str] = []
    for sub in (idle, loaded, bulk_res):
        warnings.extend(list(sub.get("warnings", [])))

    # De-dup while preserving order.
    seen: set[str] = set()
    warnings = [w for w in warnings if not (w in seen or seen.add(w))]

    out = {"metrics": metrics, "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res}}
    if warnings:
        out["warnings"] = warnings
    return out
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: PASS.

**Step 5: Commit**

```bash
git commit -m "feat(bench): surface control_under_load warnings"
```

---

### Task 4: Make `asyncio_tcp.create_self_pair()` cancellation/exception-safe

**Files:**
- Modify: `bench/comparators/asyncio_tcp.py:67`
- Test: `tests/test_bench_timeouts.py`

**Step 1: Write the failing test**

Cancel `create_self_pair()` while it is awaiting `loop.sock_connect()` and assert it does not leave the internal `accept_one` task behind.

```python
import asyncio

import pytest

from bench.comparators.asyncio_tcp import create_self_pair


@pytest.mark.asyncio
async def test_asyncio_tcp_create_self_pair_cancellation_cleans_accept_task(monkeypatch) -> None:
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
    task = asyncio.create_task(create_self_pair({"tcp_nodelay": True}))
    await connect_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    after = set(asyncio.all_tasks())
    new_tasks = [t for t in (after - before) if not t.done()]
    accept_tasks = []
    for t in new_tasks:
        coro = t.get_coro()
        name = getattr(getattr(coro, "cr_code", None), "co_name", "")
        if name == "accept_one":
            accept_tasks.append(t)

    # Cleanup even when failing so the suite can continue.
    for t in accept_tasks:
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
    assert not accept_tasks
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: FAIL; the leaked accept task is still pending.

**Step 3: Write minimal implementation**

Update `bench/comparators/asyncio_tcp.py` `create_self_pair()` to clean up partial resources on any `BaseException`:

- Always close `listener`.
- If `accept_task` exists and the function is failing/cancelled, cancel + await it.
- Close `client` / `server_conn` sockets if they were created.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bench_timeouts.py -q`
Expected: PASS.

**Step 5: Commit**

```bash
git commit -m "fix(bench): cleanup asyncio_tcp pair on cancel"
```

---

### Task 5: Make `starway_tcp.create_self_pair()` cancellation/exception-safe

**Files:**
- Modify: `bench/comparators/starway_tcp.py:84`
- Test: (optional) `tests/test_bench_timeouts.py`

**Step 1: Implement best-effort cleanup on `BaseException`**

Wrap the body in `try/except BaseException` and always attempt:

```python
    with contextlib.suppress(Exception):
        await asyncio.wait_for(client.aclose(), timeout=1.0)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(server.aclose(), timeout=1.0)
```

Remove the timeout branch that explicitly closes; let the shared cleanup handle all failures.

**Step 2: Run existing tests**

Run: `uv run pytest -q`
Expected: PASS.

**Step 3: Commit**

```bash
git commit -m "fix(bench): cleanup starway_tcp pair on cancel"
```

---

### Task 6: Ignore benchmark artifacts by default

**Files:**
- Modify: `.gitignore`

**Step 1: Add ignores**

Append to `.gitignore`:

```
results/
```

**Step 2: Sanity check**

Run: `git status`
Expected: existing `results/` no longer shows as untracked.

**Step 3: Commit**

```bash
git commit -m "chore: ignore bench results directory"
```

---

### Verification Checklist (after all tasks)

- Run: `uv run pytest -q`
Expected: PASS.

- Run: `python bench/run.py --config bench/configs/baseline_tcp_self_v1.toml --dry-run`
Expected: prints points and exits 0.

- Run: `python bench/run.py --config bench/configs/baseline_tcp_self_v1.toml`
Expected: completes; per-point JSON contains `status` + `metrics`, and any clamps/oddities appear in top-level `warnings`.

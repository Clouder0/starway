# Starway Bench MVP (Self-to-Self) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a new `bench/run.py` runner that executes a config-defined benchmark suite in **self-to-self** mode (single host), runs the previously discussed communication workloads (ML-like + diagnostic), and writes local result artifacts.

**Architecture:** Keep a small Functional Core / Imperative Shell split:

- Functional Core: config parsing/validation, manifest expansion (workloads → concrete points), deterministic run IDs.
- Imperative Shell: run loop (timing, async execution, env var setup, error handling), result writing.

**Tech Stack:** Python 3.11+, `asyncio`, `numpy`, Starway (`starway.Client`, `starway.Server`), stdlib sockets for asyncio TCP baseline.

---

## Scope

### In scope (MVP)

- A new top-level `bench/` folder containing a runnable `bench/run.py`.
- Config-driven execution; no “backend/comparator” CLI flag (comparators live in config).
- **Self-to-self only**: the runner creates both endpoints locally and runs workloads.
- Workloads implemented (previously discussed):
  - `control_rtt` (ping-pong RTT)
  - `small_message_pressure` (many small messages, delivery-complete)
  - `bulk_stream` (delivery-complete goodput, time-based)
  - `bucket_duplex_step` (bucketized duplex “step”, step-based)
  - `control_under_load` (control RTT while bulk stream runs)
  - `mixed_size` (size distribution stream, delivery-complete)
- Results written locally in a deterministic structure suitable for later gather/plot.

### Explicitly out of scope (MVP)

- Result gathering/aggregation and plotting.
- Real cluster execution (multi-host / N-node): only the abstraction stubs exist.
- RDMA / shared-memory transport comparisons (can be added after baseline TCP).
- Ray comparator.

---

## Acceptance Criteria (MVP)

1. `python bench/run.py --config bench/configs/baseline_tcp_self_v1.toml` runs to completion on one host.
2. The run executes all workloads listed in scope (at least one point per workload).
3. Artifacts are written to the configured results directory, including:
   - a top-level run metadata file
   - per-point JSON result files with status + metrics + config snapshot
   - an events log capturing failures/timeouts
4. Cluster mode is *present as an abstraction* (types/interfaces + TODO stub), but selecting it fails fast with a clear “not implemented in MVP” error.

---

## Progress

- [x] Task 1: Add config + type system scaffolding
- [x] Task 2: Implement manifest expansion (matrix → concrete points)
- [x] Task 3: Implement result writer
- [x] Task 4: Implement asyncio TCP comparator
- [x] Task 5: Implement Starway TCP comparator (lazy import)
- [x] Task 6: Implement workloads
- [x] Task 7: Implement `bench/run.py` orchestration (self-only)

## High-Level Design

### Concepts

- **Campaign**: one execution of a case family from a single config file.
- **Comparator**: an implementation under test (e.g. `asyncio_tcp`, `starway_tcp`). Selected via config.
- **Workload**: a communication pattern with fixed semantics and its own parameters.
- **Point**: one parameterization of a workload.
- **Manifest**: the expanded, explicit list of concrete runs: `(comparator, workload, point, repeat)`.

### Stop Conditions (no fixed “1GiB” totals)

- Latency workloads: `iterations`.
- Throughput workloads: `duration_s` with optional `min_bytes` guard.
- Step workloads: `steps` with per-step `bucket_bytes` and `buckets_per_step`.

### Delivery-Complete Semantics (for non-RTT workloads)

- Use **cumulative ACK** every N messages (count-based) plus a **final ACK**.
- No per-message RTT taxation for throughput.
- Encode ACK progress in the control-plane message itself (sequence number); payload can be empty.

### Self-to-Self Execution

- Run both endpoints inside the same Python process/event loop.
- Use a real TCP connection to loopback for the asyncio baseline.
- Use Starway socket-address mode with `UCX_TLS=tcp` for the Starway TCP comparator.

> Note: self-to-self in one process is an MVP convenience. The design should avoid assumptions that prevent later “one process per endpoint” execution.

---

## Files & Module Layout (MVP)

- Create: `bench/run.py`
- Create: `bench/config.py`
- Create: `bench/planner.py`
- Create: `bench/results.py`
- Create: `bench/types.py`
- Create: `bench/comparators/__init__.py`
- Create: `bench/comparators/asyncio_tcp.py`
- Create: `bench/comparators/starway_tcp.py`
- Create: `bench/workloads.py`
- Create: `bench/protocol.py`
- Create: `bench/configs/baseline_tcp_self_v1.toml`

Tests (MVP-appropriate):

- Create: `tests/test_bench_planner.py` (pure expansion + stable IDs)
- Create: `tests/test_bench_results_schema.py` (writer produces required keys)

Manual verification command (acceptance):

- Run: `python bench/run.py --config bench/configs/baseline_tcp_self_v1.toml`

---

## Result Artifacts (MVP)

Directory layout (deterministic):

```
results/
  <campaign_id>/
    run_meta.json
    events.jsonl
    manifest.json
    points/
      <point_id>.json
```

`run_meta.json` (run-level metadata):

- campaign id, start/end timestamps
- git SHA (if available), python version, platform
- environment snapshot of relevant vars (`UCX_TLS`, `STARWAY_USE_SYSTEM_UCX`, etc.)
- config hash + a copy of the effective config

Each `points/<point_id>.json` contains:

- identifiers: `campaign_id`, `comparator`, `workload`, `repeat`, `point_params`
- status: `ok|failed|skipped` + error string if failed
- metrics: workload-specific but must include canonical fields where applicable:
  - latency: `rtt_p50_us`, `rtt_p95_us`, `rtt_p99_us`, `rtt_p999_us` (if computed)
  - throughput: `goodput_gbps`, `messages_per_second`, `bytes_delivered`
  - efficiency: optional `cpu_user_s`, `cpu_system_s` (best-effort in MVP)
- samples: optional (keep small in MVP; e.g. store a few percentile inputs or windowed throughput)

`events.jsonl` captures structured events:

- run start/finish, point start/finish
- timeouts
- exceptions with context

---

## Workload Specs (MVP)

All workloads run on a local pair of endpoints.

### 1) `control_rtt` (ping-pong)

- Pattern: A sends `PING`, B replies `PONG`.
- Parameters:
  - `msg_bytes`
  - `warmup_iters`
  - `iters`
- Metrics: RTT percentiles + min/max.

### 2) `small_message_pressure` (delivery-complete)

- Pattern: A streams many small messages; B receives and sends cumulative ACK every N messages.
- Stop: `duration_s` (plus optional `min_messages`).
- Parameters:
  - `msg_bytes`, `inflight`, `duration_s`
  - `ack_every_n`
- Metrics: `messages_per_second`, `goodput_gbps`, latency percentiles if sampled.

### 3) `bulk_stream` (delivery-complete)

- Pattern: A streams `chunk_bytes` repeatedly for `duration_s`; B ACKs cumulatively.
- Stop: `duration_s` + optional `min_bytes`.
- Parameters:
  - `chunk_bytes`, `inflight`, `duration_s`, `min_bytes`
  - `ack_every_n`
- Metrics: `goodput_gbps`, `bytes_delivered`.

### 4) `bucket_duplex_step` (step-based duplex)

- Pattern: both sides exchange `buckets_per_step` buckets per step.
- Stop: `steps`.
- Parameters:
  - `bucket_bytes`, `buckets_per_step`, `steps`, `inflight`
- Metrics:
  - per-step completion time percentiles
  - effective `aggregate_gbps` per step (2 directions)

### 5) `control_under_load`

- Pattern: run `bulk_stream` in background while also running `control_rtt` periodically.
- Stop: `duration_s`.
- Parameters:
  - `bulk_*` params + `control_*` params
- Metrics:
  - `control_rtt_p99_us_loaded`
  - `control_rtt_p99_us_idle` (measured in a short pre-phase)
  - `p99_inflation_ratio`
  - background `goodput_gbps`

### 6) `mixed_size`

- Pattern: A sends messages with a configured discrete size distribution.
- Stop: `duration_s` + optional `min_bytes`.
- Parameters:
  - `sizes_bytes` + `weights`
  - `inflight`, `ack_every_n`, `duration_s`
- Metrics: `goodput_gbps`, `messages_per_second`, plus optional size histogram.

---

## Comparator Specs (MVP)

### `asyncio_tcp`

- Standard library only.
- Use non-blocking sockets and `asyncio` loop (`loop.sock_sendall`, `loop.sock_recv_into`) to allow preallocated buffers.
- Framing: fixed header with `{kind, seq, length}` then payload.
- Demux: a single reader task per peer that routes messages into `asyncio.Queue` per kind.

### `starway_tcp`

- Use `UCX_TLS=tcp`.
- Socket-address connection mode (`Server.listen` + `Client.aconnect`).
- Use tag encoding for `{kind, seq}`:
  - upper bits = kind
  - lower bits = seq
  - receiver filters by kind using `tag_mask`.
- Preallocated NumPy `uint8` buffers; send slices for variable sizes.
- Ensure UCX env vars are set **before importing `starway`**.

---

## Cluster Abstraction (TODO in MVP)

Define interfaces/types now (but do not implement behavior):

- `ExecutionMode = self | cluster`
- `Executor` interface with `run_manifest(manifest) -> None`
- `SelfExecutor` implemented.
- `ClusterExecutor` raises `NotImplementedError("cluster mode is TODO")`.

Keep the planner/manifest format compatible with future N-node scheduling:

- a point can declare `participants: ["nodeA", "nodeB", ...]`
- self-mode uses implicit participants `["self"]`

---

## Detailed Tasks (bite-sized)

### Task 1: Add config + type system scaffolding

**Files:**

- Create: `bench/types.py`
- Create: `bench/config.py`
- Test: `tests/test_bench_planner.py`

**Step 1: Write failing test for config parsing**

```python
def test_load_config_minimal(tmp_path):
    ...
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench_planner.py -q`
Expected: FAIL (missing module/functions).

**Step 3: Implement minimal config loader + dataclasses**

- Parse TOML
- Validate required keys
- Apply defaults

**Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_bench_planner.py -q`
Expected: PASS.

**Step 5: (Optional) Commit**

`git commit -m "feat(bench): add config/types scaffolding"`

### Task 2: Implement manifest expansion (matrix → concrete points)

**Files:**

- Create: `bench/planner.py`
- Modify: `tests/test_bench_planner.py`

**Step 1: Write failing test for stable manifest expansion**

Test should assert:

- expected number of points
- stable `point_id` generation

**Step 2: Run tests (should fail)**

Run: `uv run pytest tests/test_bench_planner.py -q`

**Step 3: Implement planner**

- Expand `comparators × workloads × workload_points × repeats`
- Sort deterministically
- Generate `point_id` from a stable hash of `(campaign_id, comparator, workload, params, repeat)`

**Step 4: Run tests (should pass)**

Run: `uv run pytest tests/test_bench_planner.py -q`

### Task 3: Implement result writer

**Files:**

- Create: `bench/results.py`
- Test: `tests/test_bench_results_schema.py`

**Step 1: Write failing test**

- create temp results dir
- write one ok point
- assert files exist and contain required keys

**Step 2: Run test (fail)**

Run: `uv run pytest tests/test_bench_results_schema.py -q`

**Step 3: Implement writer**

- create directories
- write `run_meta.json`, `manifest.json`
- append `events.jsonl`
- write point JSON per `point_id`

**Step 4: Run test (pass)**

Run: `uv run pytest tests/test_bench_results_schema.py -q`

### Task 4: Implement asyncio TCP comparator

**Files:**

- Create: `bench/comparators/asyncio_tcp.py`
- Modify: `bench/comparators/__init__.py`
- Create: `bench/protocol.py`

**Step 1: Implement minimal framed message protocol**

- header pack/unpack
- message kinds

**Step 2: Implement self-to-self pair creation**

- start server socket on loopback
- connect client socket
- return two peer objects with `send(kind, seq, buf_view)` and `recv(kind, buf)`

**Step 3: Add internal demux**

- background reader per peer
- route by kind to per-kind queues

**Step 4: Manual smoke**

Run a tiny send/recv ping inside a small script or via `bench/run.py` once wired.

### Task 5: Implement Starway TCP comparator (lazy import)

**Files:**

- Create: `bench/comparators/starway_tcp.py`

**Step 1: Ensure env is applied before import**

- Set `UCX_TLS=tcp` (and other comparator env) before importing `starway`.

**Step 2: Implement self-to-self pair creation**

- `Server.listen("127.0.0.1", port)`
- `Client.aconnect(...)`
- obtain `ServerEndpoint` (`next(iter(server.list_clients()))`)
- return two peer wrappers

### Task 6: Implement workloads (one by one)

**Files:**

- Create: `bench/workloads.py`

Implement in this order:

1. `control_rtt` (foundation)
2. `bulk_stream` (foundation for under-load)
3. `small_message_pressure`
4. `mixed_size`
5. `control_under_load` (composes `control_rtt` + `bulk_stream`)
6. `bucket_duplex_step` (most complex)

For each workload:

**Step 1: Add a tiny unit-level harness (optional)**

- Validate parameter sanity and metric keys without running UCX.

**Step 2: Implement workload against the peer interface**

**Step 3: Run a small local campaign**

- Use `asyncio_tcp` only first.
- Then enable `starway_tcp` and verify it completes.

### Task 7: Implement `bench/run.py` orchestration (self-only)

**Files:**

- Create: `bench/run.py`
- Create: `bench/configs/baseline_tcp_self_v1.toml`

**Step 1: CLI boundary**

- `--config` required
- optional `--out` override (nice-to-have; can omit in MVP)

**Step 2: Load config and build manifest**

- write `run_meta.json` + `manifest.json`

**Step 3: SelfExecutor happy path**

- for each point:
  - instantiate comparator pair
  - run workload
  - write point result
  - close resources

**Step 4: ClusterExecutor stub**

- if config selects cluster mode, raise with clear TODO.

**Step 5: Acceptance run**

Run: `python bench/run.py --config bench/configs/baseline_tcp_self_v1.toml`
Expected: completes with results written under `results/<campaign_id>/...`.

---

## MVP Config (initial, small)

Create `bench/configs/baseline_tcp_self_v1.toml` with:

- comparators: `asyncio_tcp`, `starway_tcp`
- workloads: include all 6 workloads, but keep each with 1-2 points
- repeats: 1-3
- short durations (seconds) by default so MVP completes quickly

---

## Next Steps After MVP (not part of this plan)

- Implement cluster mode executor (N-node scheduling + barriers).
- Add transport/capability tracks: `starway_sm`, `starway_rdma`.
- Add gather/plot pipeline.
- Add Ray comparator.

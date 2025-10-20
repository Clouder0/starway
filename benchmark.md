# Starway Benchmark Suite

This document captures the scenarios, metrics, and workflow for the benchmark/perftest
utility that ships with Starway. The goal is to let users quickly evaluate UCX
transports (TCP, shared memory, InfiniBand/RDMA, and future options) under
workloads that mirror real, latency-sensitive machine learning traffic patterns.

## Quick Start

- Ensure UCX is installed and Starway builds against the desired transport.
- Choose a transport via the `--tls` flag (preferred) or by exporting
  `UCX_TLS`. Examples: `tcp`, `sm`, `rc`, `cuda_copy`.
- Run the CLI on both peers:
  - Server (listener or worker address mode):
    ```
    uv run python -m starway.bench --role server --addr 0.0.0.0 --port 17777 --tls tcp
    ```
  - Client:
    ```
    uv run python -m starway.bench --role client --server-host 192.168.1.10 --port 17777 --tls tcp --scenarios all
    ```
- For loopback/local evaluation, a single process can orchestrate both endpoints:
  ```
  uv run python -m starway.bench --role loopback --tls sm --scenarios large-array small-messages
  ```

Each run prints per-scenario statistics (bandwidth, latency, throughput) and a
summary table. Results are saved to JSON when `--output` is provided.

## Orchestration Model

The benchmark suite uses a lightweight control channel over UCX tags:

1. The server starts listening (TCP socket or UCX worker address).
2. The client connects, sends a control frame describing the scenario, and
   timestamps locally.
3. The server posts receives ahead of data transfer and, where needed, responds
   with acknowledgements.
4. The client aggregates per-iteration measurements and reports summary
   statistics (min/median/max, percentiles in future work).

All measurements are done in the Python process where the action happens
(send-side for bandwidth, round trip for latency). UCX flushes are issued before
timestamps are finalized to ensure the data is on the wire.

## Scenario Catalogue

### `large-array`

**Purpose**: Measure one-way bandwidth for a single contiguous buffer – typical
for checkpointing or tensor broadcast.

- Direction: Client → Server.
- Payload: Configurable, default 1 GiB (using pinned NumPy `uint8` array).
- Iterations: warmup (1), measured repetitions (3 by default).
- Metrics: GB/s transfer rate, total time, UCX flush completion latency.
- Transport considerations:
  - TCP: tests NIC bandwidth.
  - `sm`: shared-memory path between colocated processes.
  - `rc`/`ud`: RDMA; verify fabric health.

### `small-messages`

**Purpose**: Stress many concurrent control-sized updates such as optimizer
state deltas or parameter server RPCs.

- Direction: Client → Server.
- Payload: 1 KiB messages by default (configurable), dispatched from multiple
  asyncio tasks.
- Concurrency: Default 64 outstanding sends (tunable via `--concurrency`).
- Iterations: warmup round plus 10 timed batches.
- Metrics: messages per second, effective bandwidth, 50/95 percentile latency
  approximated from aggregated timings.
- Transport considerations:
  - `tcp`: measures Nagle/latency interactions (disables via UCX settings).
  - `sm`: exercises shared-memory active messages.

### `pingpong-flag`

**Purpose**: Capture synchronization latency by bouncing a single byte control
flag between peers.

- Flow: Client sends flag, server responds immediately; both sides use
  pre-allocated 1-byte buffers.
- Iterations: 100 warmup, 1000 measured exchanges by default.
- Metrics: round-trip latency (mean, min, max) and derived one-way latency.
- Transport considerations:
  - For `rc`, surfaces RDMA completion behavior.
  - `sm` highlights intra-node scheduling costs.

### `streaming-duplex`

**Purpose**: Simulate bidirectional tensor streaming (e.g., model parallelism,
gradient + activation exchange).

- Flow: Client and server both stream medium-sized chunks (4 MiB default) in
  parallel; each side posts receives and sends `--iterations` chunks.
- Metrics: aggregate throughput in each direction and combined GB/s. Measures
  fairness and potential head-of-line blocking across UCX endpoints.
- Transport considerations:
  - RDMA fabrics should approach line rate in both directions.
  - TCP may exhibit asymmetry on lossy networks.

### Extensibility

The CLI accepts `--scenarios` as a space-separated list; additional scenarios
can be registered by editing `src/starway/benchmarks/scenarios.py`. Ideas for
future coverage include:

- GPU → GPU transfers (using CUDA buffers when `STARWAY_USE_SYSTEM_UCX=false`).
- Injection rate sweeps to find latency/bandwidth crossover points.
- Payload integrity verification (CRC) for diagnosing packet corruption.

## Transport Selection

UCX uses the `UCX_TLS` environment variable to choose transports. The CLI
propagates the value supplied via `--tls` before the Starway bindings are
imported. Examples:

- `--tls tcp`: Force TCP only.
- `--tls sm`: Shared memory – use for loopback on the same node.
- `--tls rc,ud`: RDMA RoCE/InfiniBand (requires fabric support).

When omitted, the current environment is respected. Extra UCX tuning knobs such
as `UCX_SOCKADDR_CM_ENABLE=y`, `UCX_LOG_LEVEL=INFO`, or `UCX_NET_DEVICES` can be
passed via the shell environment prior to launching the benchmark.

## Output & Post-processing

- Console output includes per-scenario measurements and a JSON summary.
- `--output /path/to/results.json` writes the report for later comparison.
- The report captures:
  - Scenario metadata (transport, message size, concurrency, iterations).
  - Timing statistics.
  - Raw iteration traces for external plotting (optional via `--store-trace`).

## Operational Recommendations

- Run each transport in isolation to minimize NIC contention.
- Pin processes to CPUs using `taskset`/`numactl` when comparing transports.
- For InfiniBand/RoCE, ensure `ibv_devinfo` is healthy and UCX can reach the
  devices (`UCX_NET_DEVICES=mlx5_0:1` etc.).
- For reproducibility, note firmware, driver, and UCX versions in reports.

## Limitations & Future Work

- Current implementation targets CPU-based buffers; CUDA buffers are planned.
- Percentile calculations are coarse; integrating HDR histograms is a future
  enhancement.
- The CLI operates over a single connection pair; for many-to-many topologies,
  users should orchestrate multiple clients.

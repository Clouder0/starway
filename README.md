# Starway

Starway is an ultra-fast communication library that wraps OpenUCX to deliver
lock-free, asynchronous, zero-copy messaging for Python applications.

## Highlights

- Zero-copy transfers into preallocated NumPy buffers.
- Full-duplex messaging with independent async send/recv APIs.
- Choice of connection style: traditional TCP socket address or direct UCX
  worker-address handshakes (no TCP listener required).
- Flush primitives (`aflush`, `aflush_ep`) to force completion ordering when
  you need delivery guarantees.
- Built-in performance probes via `evaluate_perf` for quick transport telemetry.

## Installation

Starway depends on dynamic OpenUCX libraries:

1. Install OpenUCX system-wide (for example via your distro's package manager
   or an HPC toolchain), or
2. Install the `libucx-cu12` wheel, which ships redistributable shared objects.

Starway does not hard-depend on `libucx-cu12`; it is treated as an optional
fallback.

You can use environment variable to control System/Wheel preference:

```python
import os

# Defaults to "true". Set to "false" to prefer the wheel first.
os.environ["STARWAY_USE_SYSTEM_UCX"] = "false"

import starway  # falls back to system UCX if the wheel is unavailable
```

## Quick Start

### Socket-address workflow

```python
import asyncio
import numpy as np
from starway import Client, Server

SERVER_ADDR, SERVER_PORT = "127.0.0.1", 19198


async def main():
    server = Server()
    client = Client()

    server.listen(SERVER_ADDR, SERVER_PORT)
    await client.aconnect(SERVER_ADDR, SERVER_PORT)

    # Establish endpoint object on the server side
    client_ep = next(iter(server.list_clients()))

    send_buf = np.arange(4, dtype=np.uint8)
    recv_buf = np.zeros_like(send_buf)

    recv_task = server.arecv(recv_buf, tag=1, tag_mask=0xFFFF)
    await client.asend(send_buf, tag=1)
    await recv_task

    await asyncio.gather(client.aflush(), server.aflush_ep(client_ep))
    await asyncio.gather(client.aclose(), server.aclose())


asyncio.run(main())
```

### Worker-address workflow

```python
import asyncio
import numpy as np
from starway import Client, Server


async def main():
    server = Server()
    worker_address = server.listen_address()  # bytes blob, no TCP listener

    client = Client()
    await client.aconnect_address(worker_address)

    # Accept callback still fires; poll list_clients() to obtain the peer ep
    for _ in range(100):
        clients = server.list_clients()
        if clients:
            client_ep = next(iter(clients))
            break
        await asyncio.sleep(0.01)

    recv_buf = np.zeros(4, dtype=np.uint8)
    recv_task = server.arecv(recv_buf, tag=2, tag_mask=0xFFFF)
    await client.asend(np.arange(4, dtype=np.uint8), tag=2)
    await recv_task

    await asyncio.gather(client.aclose(), server.aclose())


asyncio.run(main())
```

## API Cheatsheet

### `Server`

- `listen(addr, port)` – create a TCP-backed listener.
- `listen_address()` – start worker-only mode and return the local UCX worker
  address for out-of-band distribution.
- `set_accept_cb(callback)` – invoked for each new endpoint (both listener and
  worker-address flows).
- `list_clients()` – inspect active UCX endpoints.
- `asend(client_ep, buffer, tag)` / `arecv(buffer, tag, tag_mask)` – async
  one-sided messaging.
- `aflush()` – wait for all server-side operations to complete.
- `aflush_ep(client_ep)` – flush operations targeting a specific endpoint.
- `evaluate_perf(client_ep, msg_size)` – request UCX transport estimates.
- `get_worker_address()` – read back the cached worker address bytes.
- `aclose()` – gracefully shut down and release resources.

### `Client`

- `aconnect(addr, port)` – connect to a listener.
- `aconnect_address(worker_address)` – connect using a UCX worker address only.
- `asend(buffer, tag)` / `arecv(buffer, tag, tag_mask)` – async messaging APIs.
- `aflush()` – wait for in-flight client operations to finish.
- `evaluate_perf(msg_size)` – ask UCX for local transport estimates.
- `get_worker_address()` – shareable bytes for reciprocal connections.
- `aclose()` – close the connection and stop the worker.

### `ServerEndpoint`

Inspect transport metadata for debugging or topology decisions:

- `.name` – UCX endpoint name.
- `.local_addr`, `.local_port`, `.remote_addr`, `.remote_port` – socket details
  when available (address-only mode may leave these empty).
- `.view_transports()` – list `(device, transport)` tuples negotiated by UCX.

## Flushing and Ordering

UCX operations are inherently asynchronous. Starway exposes lightweight futures
on top of UCX requests and lets you opt into ordering guarantees:

- `Client.aflush()` and `Server.aflush()` drain all pending operations on their
  respective workers.
- `Server.aflush_ep(ep)` waits for sends targeting a single endpoint.
- All async flushes integrate with Python's event loop, making them suitable for
  structured concurrency patterns (`await asyncio.gather(...)`).

## Testing

Pytest drives the integration suite. Typical commands:

```bash
uv sync --group dev --group test
uv run pytest tests/test_basic.py -vv
```

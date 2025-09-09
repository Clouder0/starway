# Starway

Starway aims to be an ultra-fast communication library, which features:

1. Zero Copy, supports sending from pointer and receiving into pre-allocated buffer.
2. RDMA support, by utilizing OpenMPI/OpenUCX for transportation.
3. Ease of use, generally should work out of the box, and don't require much configuration efforts.
4. Full-duplex, asynchronous API.

Current Python alternatives are lacking core features:

1. ZeroMQ, no support for RDMA.
2. MPI, hard to use, hard to setup environment properly.

## Installation

Starway depends on OpenUCX, which must be linked dynamically. There are two options:

1. Install OpenUCX system-wide, or whatever method you like as long as it can be found dynamically.
2. Install `libucx-cu12` wheel package, which contains `libucx.so` files that can be loaded by Starway during initialization.

We don't add `libucx-cu12`  as Starway Python dependency by default, but generally you can install it on your cluster machines as a fallback option: when libucx cannot be found in system, it would look for wheel installation.

You can use environment variable to control System/Wheel preference:

```py
import os
os.environ["STARWAY_USE_SYSTEM_UCX"] = "false" # defaults to true
import starway  # now we will use libucx-cu12 pypi wheel package, while falling back to system if not found
```

## Full-Duplex Communication

Starway now supports full-duplex communication, allowing for simultaneous, two-way data exchange between the client and server. This feature significantly improves the library's responsiveness and efficiency in handling real-time data streams.

### Usage

Here's an example of how to use the full-duplex communication feature:

```python
import asyncio
import time
import numpy as np
from starway import Client, Server

async def tester():
    server = Server("127.0.0.1", 19198)
    client = Client("127.0.0.1", 19198)
    
    # Client to Server
    send_buf_c2s = np.arange(10, dtype=np.uint8)
    recv_buf_c2s = np.empty(10, dtype=np.uint8)
    
    # Server to Client
    send_buf_s2c = np.arange(20, dtype=np.uint8)
    recv_buf_s2c = np.empty(10, dtype=np.uint8)

    # Wait for client to connect
    while not (clients := server.list_clients()):
        time.sleep(0.1)
    client_ep = clients[0]

    # Perform concurrent send and receive
    client_send_future = client.asend(send_buf_c2s, tag=1)
    server_recv_future = server.arecv(recv_buf_c2s, tag=1, tag_mask=0xFFFF)
    server_send_future = server.asend(client_ep, send_buf_s2c, tag=2)
    client_recv_future = client.arecv(recv_buf_s2c, tag=2, tag_mask=0xFFFF)

    await asyncio.gather(
        client_send_future,
        server_recv_future,
        server_send_future,
        client_recv_future
    )

    assert np.allclose(send_buf_c2s, recv_buf_c2s)
    assert np.allclose(send_buf_s2c, recv_buf_s2c)
    
asyncio.run(tester())

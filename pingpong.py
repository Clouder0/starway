import asyncio
import random
import time

import numpy as np
import uvloop

from starway import Client, Server


async def main():
    port = random.randint(10000, 50000)
    client = Client()
    server = Server()
    server.listen("127.0.0.1", port)
    await client.aconnect("127.0.0.1", port)

    msg_size = [
        1,
        64,
        128,
        1024,
        1024 * 64,
        1024 * 128,
        1024 * 512,
        1024 * 1024,
        1024 * 1024 * 50,
        1024 * 1024 * 1024,
    ]
    for msg in msg_size:
        send = np.ones(msg, dtype=np.uint8) + 2
        recv = np.empty(msg, dtype=np.uint8)
        recv.fill(0)
        t = client.evaluate_perf(msg)
        t0 = time.time()
        await asyncio.gather(server.arecv(recv, 1, 0), client.asend(send, 1))
        t1 = time.time()
        print(
            f"msg size {msg}, perf {t:.3}s, throughput: {msg / (t + 1e-18) / 1024 / 1024 / 1024 * 8:.2f} Gbps; Custom throughput: {msg / (t1 - t0) / 1024 / 1024 / 1024 * 8:.2f}Gbps."
        )

    await client.aclose()
    await server.aclose()


if __name__ == "__main__":
    uvloop.run(main())

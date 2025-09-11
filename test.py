import asyncio
import random
import time

import numpy as np
import uvloop

from starway import Client, Server

p = random.randint(10000, 11000)
p2 = random.randint(10000, 11000)


total_send_done = 0
total_recv_done = 0


def test_async():
    async def tester():
        server = Server()
        server.listen("127.0.0.1", p2)
        await asyncio.sleep(0.2)
        client = Client()
        await client.aconnect("127.0.0.1", p2)
        await asyncio.sleep(0.2)
        # concurrent sends
        concurrency = 5
        single_pack = 1024 * 1024 * 1024
        to_sends = [np.ones(single_pack, dtype=np.uint8) for i in range(concurrency)]
        print("Allocated.")

        t0 = time.time()
        send_futures = [client.asend(to_sends[i], i) for i in range(concurrency)]
        to_recvs = [np.empty(single_pack, np.uint8) for i in range(concurrency)]
        recv_futures = [server.arecv(to_recvs[i], i, 0) for i in range(concurrency)]
        await asyncio.gather(*send_futures, *recv_futures)
        t1 = time.time()
        print(
            "Cost",
            t1 - t0,
            "seconds",
            "Throughput: ",
            single_pack * concurrency / (t1 - t0) / 1024 / 1024 / 1024 * 8,
            "Gbps",
        )
        # for i in range(concurrency):
        # assert np.allclose(to_sends[i], to_recvs[i])

        print("All done, closing")
        await client.aclose()
        await server.aclose()

    uvloop.run(tester())


test_async()
# test_basic()

import asyncio
import random
import time

import numpy as np

from starway import Client, Server

p = random.randint(10000, 11000)
p2 = random.randint(10000, 11000)


def test_basic():
    server = Server("127.0.0.1", p)
    client = Client("127.0.0.1", p)
    some_buffer = np.arange(12345, dtype=np.uint8)
    recv_buffer = np.empty(12345, np.uint8)
    send_future = client.send(some_buffer, 123)
    recv_future = server.recv(recv_buffer, 123, 0xFFFF)
    while not send_future.done():
        pass
    while not recv_future.done():
        pass
    assert np.allclose(some_buffer, recv_buffer)


total_send_done = 0
total_recv_done = 0


def test_async():
    async def tester():
        server = Server("127.0.0.1", p2)
        client = Client("127.0.0.1", p2)
        # concurrent sends
        concurrency = 100
        single_pack = 1024 * 1024 * 10
        to_sends = [
            np.arange(single_pack, dtype=np.uint8) * i for i in range(concurrency)
        ]
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
        for x in send_futures:
            assert x.done()
            assert x.exception() is None

        for i, x in enumerate(recv_futures):
            assert x.done()
            assert x.exception() is None
            tag, length = x.result()
            assert tag == i
            assert length == single_pack
        for i in range(concurrency):
            assert np.allclose(to_sends[i], to_recvs[i])

    asyncio.run(tester())


test_async()
# test_basic()

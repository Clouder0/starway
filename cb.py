import asyncio
import random
import time

import numpy as np

from starway import Client, Server

port = random.randint(10000, 50000)


def test_async(port):
    async def tester():
        server = Server()
        server.listen("127.0.0.1", port)
        await asyncio.sleep(0.01)
        client = Client()
        await client.aconnect("127.0.0.1", port)
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
        recv_futures = [
            server.arecv(to_recvs[i], i, 0xFFFF) for i in range(concurrency)
        ]
        # full duplex test
        server_send_buf = [
            np.arange(single_pack, dtype=np.uint8) * (i + 10)
            for i in range(concurrency)
        ]
        server_recv_buf = [np.empty(single_pack, np.uint8) for i in range(concurrency)]
        while not (clients := server.list_clients()):
            time.sleep(0.1)
        client_ep = next(iter(clients))
        server_send_futures = [
            server.asend(client_ep, server_send_buf[i], i + 10)
            for i in range(concurrency)
        ]
        client_recv_futures = [
            client.arecv(server_recv_buf[i], i + 10, 0xFFFF) for i in range(concurrency)
        ]

        await asyncio.gather(
            *send_futures, *recv_futures, *server_send_futures, *client_recv_futures
        )
        t1 = time.time()
        print(
            "Cost",
            t1 - t0,
            "seconds",
            "Throughput: ",
            single_pack * concurrency / (t1 - t0) / 1024 / 1024 / 1024 * 8 * 2,
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

        for x in server_send_futures:
            assert x.done()
            assert x.exception() is None

        for i, x in enumerate(client_recv_futures):
            assert x.done()
            assert x.exception() is None
            tag, length = x.result()
            assert tag == i + 10
            assert length == single_pack

        for i in range(concurrency):
            assert np.allclose(server_send_buf[i], server_recv_buf[i])
        await client.aclose()
        await server.aclose()

    asyncio.run(tester())


test_async(port)

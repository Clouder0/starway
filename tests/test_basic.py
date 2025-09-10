import asyncio
import random
import time

import numpy as np
import pytest

from starway import Client, Server, ucp_get_version


@pytest.fixture
def port():
    return random.randint(10000, 60000)


def test_basic(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    some_buffer = np.arange(12345, dtype=np.uint8)
    recv_buffer = np.empty(12345, np.uint8)
    send_future = client.send(some_buffer, 123)
    recv_future = server.recv(recv_buffer, 123, 0xFFFF)
    while not send_future.done():
        pass
    while not recv_future.done():
        pass
    assert np.allclose(some_buffer, recv_buffer)


def test_async(port):
    async def tester():
        server = Server("127.0.0.1", port)
        client = Client("127.0.0.1", port)
        # concurrent sends
        concurrency = 10
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
        client_ep = clients[0]
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

    asyncio.run(tester())


def test_client_unconnected(port):
    Client("127.0.0.1", port)


def test_client_unconnected_send(port):
    client = Client("127.0.0.1", port)
    arr = np.arange(10, dtype=np.uint8)
    client.send(arr, 0)


def test_client_unconnected_recv(port):
    client = Client("127.0.0.1", port)
    arr = np.arange(10, dtype=np.uint8)
    client.recv(arr, 0, 0xFF)


def test_client_unconnected_send_recv(port):
    client = Client("127.0.0.1", port)
    arr = np.arange(10, dtype=np.uint8)
    client.recv(arr, 0, 0xFF)
    client.send(arr, 1)


def test_server_unconnected(port):
    Server("127.0.0.1", port)


def test_server_unconnected_recv(port):
    server = Server("127.0.0.1", port)
    arr = np.arange(10, dtype=np.uint8)
    server.recv(arr, 0, 0xFF)


def test_pingpong_server_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    send_buf_s = np.arange(10, dtype=np.uint8)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    recv_buf_s = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.send(server.list_clients()[0], send_buf_s, 0)
    server.recv(recv_buf_s, 0, 0)
    client.send(send_buf_c, 0)
    client.recv(recv_buf_c, 0, 0)
    del server
    del client


def test_pingpong_client_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    send_buf_s = np.arange(10, dtype=np.uint8)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    recv_buf_s = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.send(server.list_clients()[0], send_buf_s, 0)
    server.recv(recv_buf_s, 0, 0)
    client.send(send_buf_c, 0)
    client.recv(recv_buf_c, 0, 0)
    del client
    del server


def test_client_send_no_recv_client_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    client.send(send_buf_c, 0)
    del client
    del server


def test_client_send_no_recv_server_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    client.send(send_buf_c, 0)
    del server
    del client


def test_client_recv_no_send_client_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    client.recv(recv_buf_c, 0, 0)
    del client
    del server


def test_client_recv_no_send_server_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    client.recv(recv_buf_c, 0, 0)
    del server
    del client


def test_server_recv_no_send_client_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.recv(recv_buf_c, 0, 0)
    del client
    del server


def test_server_recv_no_send_server_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    recv_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.recv(recv_buf_c, 0, 0)
    del server
    del client


def test_server_send_no_recv_client_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.send(server.list_clients()[0], send_buf_c, 0)
    del client
    del server


def test_server_send_no_recv_server_exit_first(port):
    server = Server("127.0.0.1", port)
    client = Client("127.0.0.1", port)
    send_buf_c = np.arange(10, dtype=np.uint8)
    while len(server.list_clients()) < 1:
        time.sleep(0.01)
    server.send(server.list_clients()[0], send_buf_c, 0)
    del server
    del client

def test_ucp_get_version():
    ucp_get_version()
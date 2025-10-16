import asyncio
import contextlib
import gc
import multiprocessing as mp
import random

import numpy as np
import pytest

from starway import Client, Server

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio

SERVER_ADDR = "127.0.0.1"


@pytest.fixture
def port():
    return random.randint(10000, 50000)


# ==============================================================================
# Basic Functionality Tests
# ==============================================================================


@contextlib.asynccontextmanager
async def gen_server_client(port):
    server = Server()
    client = Client()

    server.listen(SERVER_ADDR, port)
    await client.aconnect(SERVER_ADDR, port)

    try:
        yield server, client
    finally:
        await client.aclose()
        await server.aclose()


async def test_server_listen_client_connect_close(port):
    server = Server()
    client = Client()

    server.listen(SERVER_ADDR, port)
    await client.aconnect(SERVER_ADDR, port)

    client_list = server.list_clients()
    assert len(client_list) == 1

    await client.aclose()

    client_list_after_close = server.list_clients()
    assert len(client_list_after_close) == 1

    await server.aclose()


async def test_client_to_server_send_recv(port):
    async with gen_server_client(port) as (server, client):
        send_buf = np.arange(10, dtype=np.uint8)
        recv_buf = np.zeros(10, dtype=np.uint8)

        # Server posts a receive buffer
        recv_task = server.arecv(recv_buf, 0, 0)
        await asyncio.sleep(0.01)

        # Client sends data
        await client.asend(send_buf, 1)

        # Wait for receive to complete
        sender_tag, length = await recv_task

        assert sender_tag == 1
        assert length == len(send_buf)
        np.testing.assert_array_equal(send_buf, recv_buf)


async def test_server_to_client_send_recv(port):
    async with gen_server_client(port) as (server, client):
        send_buf = np.arange(20, dtype=np.uint8)
        recv_buf = np.zeros(20, dtype=np.uint8)

        client_list = server.list_clients()
        assert len(client_list) > 0
        client_ep = client_list.pop()

        # Client posts a receive buffer
        recv_task = client.arecv(recv_buf, 0, 0)
        await asyncio.sleep(0.01)

        # Server sends data
        await server.asend(client_ep, send_buf, 2)

        # Wait for receive to complete
        sender_tag, length = await recv_task

        assert sender_tag == 2
        assert length == len(send_buf)
        np.testing.assert_array_equal(send_buf, recv_buf)


def server_send(port, with_flush: bool = False):
    import os

    os.environ["UCS_TLS"] = "tcp"  # force use tcp

    async def inner():
        server = Server()
        server.listen(SERVER_ADDR, port)
        connected = asyncio.Event()
        loop = asyncio.get_running_loop()

        def accept_cb(ep):
            loop.call_soon_threadsafe(connected.set)

        server.set_accept_cb(accept_cb)
        await connected.wait()
        ep = next(iter(server.list_clients()))
        send_buf = np.arange(
            1024 * 1024 * 1024 * 8, dtype=np.uint8
        )  # must be big enough to be 'on the flight'
        # this test may fail if the buffer is too small
        await server.asend(ep, send_buf, 0)
        if with_flush:
            await server.aflush()
            print("Flush done")
        await server.aclose()

    asyncio.run(inner())


def server_send_flush_ep(port, with_flush_ep: bool = False):
    import os

    os.environ["UCS_TLS"] = "tcp"  # force use tcp

    async def inner():
        server = Server()
        server.listen(SERVER_ADDR, port)
        connected = asyncio.Event()
        loop = asyncio.get_running_loop()

        def accept_cb(ep):
            loop.call_soon_threadsafe(connected.set)

        server.set_accept_cb(accept_cb)
        await connected.wait()
        ep = next(iter(server.list_clients()))
        send_buf = np.arange(
            1024 * 1024 * 1024 * 8, dtype=np.uint8
        )  # must be big enough to be 'on the flight'
        # this test may fail if the buffer is too small
        await server.asend(ep, send_buf, 0)
        if with_flush_ep:
            await server.aflush_ep(ep)
            print("Flush ep done")
        await server.aclose()

    asyncio.run(inner())


async def test_server_send_without_flush_bad(port):
    # use spawn
    ctx = mp.get_context("spawn")
    p_server = ctx.Process(target=server_send, args=(port, False))
    p_server.start()
    await asyncio.sleep(0.2)
    client = Client()
    await client.aconnect(SERVER_ADDR, port)
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    done = False

    def done_callback(sender_tag, length):
        nonlocal done
        done = True

    def fail_callback(error):
        nonlocal done
        done = True

    # Client posts a receive buffer
    client.recv(recv_buf, 0, 0, done_callback, fail_callback)
    # will never be called
    await asyncio.sleep(1.0)
    assert not done
    await client.aclose()
    p_server.kill()
    p_server.join()
    p_server.close()


async def test_server_send_with_flush_good(port):
    ctx = mp.get_context("spawn")
    p_server = ctx.Process(target=server_send, args=(port, True))
    p_server.start()
    await asyncio.sleep(0.2)
    client = Client()
    await client.aconnect(SERVER_ADDR, port)
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    # Client posts a receive buffer
    recv_future = client.arecv(recv_buf, 0, 0)
    p_server.join()
    await recv_future
    await client.aclose()
    p_server.close()


async def test_server_send_with_flush_ep_good(port):
    ctx = mp.get_context("spawn")
    p_server = ctx.Process(target=server_send_flush_ep, args=(port, True))
    p_server.start()
    await asyncio.sleep(0.2)
    client = Client()
    await client.aconnect(SERVER_ADDR, port)
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    # Client posts a receive buffer
    recv_future = client.arecv(recv_buf, 0, 0)
    p_server.join()
    await recv_future
    await client.aclose()
    p_server.close()


async def test_server_send_without_flush_ep_bad(port):
    # use spawn
    ctx = mp.get_context("spawn")
    p_server = ctx.Process(target=server_send_flush_ep, args=(port, False))
    p_server.start()
    await asyncio.sleep(0.2)
    client = Client()
    await client.aconnect(SERVER_ADDR, port)
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    done = False

    def done_callback(sender_tag, length):
        nonlocal done
        done = True

    def fail_callback(error):
        nonlocal done
        done = True

    # Client posts a receive buffer
    client.recv(recv_buf, 0, 0, done_callback, fail_callback)
    # will never be called
    await asyncio.sleep(1.0)
    assert not done
    await client.aclose()
    p_server.kill()
    p_server.join()
    p_server.close()


def client_send(port, with_flush: bool = False):
    import os

    os.environ["UCS_TLS"] = "tcp"  # force use tcp

    async def inner():
        client = Client()
        await client.aconnect(SERVER_ADDR, port)
        send_buf = np.arange(
            1024 * 1024 * 1024 * 8, dtype=np.uint8
        )  # must be big enough to be 'on the flight'
        await client.asend(send_buf, 0)
        if with_flush:
            await client.aflush()
            print("Client flush done")
        await client.aclose()

    asyncio.run(inner())


async def test_client_send_without_flush_bad(port):
    server = Server()
    server.listen(SERVER_ADDR, port)
    connected = asyncio.Event()
    loop = asyncio.get_running_loop()

    def accept_cb(_):
        loop.call_soon_threadsafe(connected.set)

    server.set_accept_cb(accept_cb)
    ctx = mp.get_context("spawn")
    p_client = ctx.Process(target=client_send, args=(port, False))
    p_client.start()
    await connected.wait()
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    done = False

    def done_callback(sender_tag, length):
        nonlocal done
        done = True

    def fail_callback(error):
        nonlocal done
        done = True

    server.recv(recv_buf, 0, 0, done_callback, fail_callback)
    await asyncio.sleep(1.0)
    assert not done
    p_client.kill()
    p_client.join()
    p_client.close()
    await server.aclose()


async def test_client_send_with_flush_good(port):
    server = Server()
    server.listen(SERVER_ADDR, port)
    connected = asyncio.Event()
    loop = asyncio.get_running_loop()

    def accept_cb(_):
        loop.call_soon_threadsafe(connected.set)

    server.set_accept_cb(accept_cb)
    ctx = mp.get_context("spawn")
    p_client = ctx.Process(target=client_send, args=(port, True))
    p_client.start()
    await connected.wait()
    recv_buf = np.zeros(1024 * 1024 * 1024 * 8, dtype=np.uint8)
    recv_future = server.arecv(recv_buf, 0, 0)
    p_client.join()
    await recv_future
    p_client.close()
    await server.aclose()


@pytest.mark.parametrize("size", [1, 1024, 4096])
async def test_message_integrity_various_sizes(port, size):
    async with gen_server_client(port) as server_and_client:
        server, client = server_and_client
        send_buf = np.random.randint(0, 256, size, dtype=np.uint8)
        recv_buf = np.zeros(size, dtype=np.uint8)

        client_list = server.list_clients()
        assert len(client_list) > 0
        client_ep = client_list.pop()

        # Test client to server
        recv_task = server.arecv(recv_buf, 0, 0)
        await client.asend(send_buf, 3)
        _, length = await recv_task
        assert length == size
        np.testing.assert_array_equal(send_buf, recv_buf)

        # Test server to client
        recv_buf.fill(0)
        recv_task = client.arecv(recv_buf, 0, 0)
        await server.asend(client_ep, send_buf, 4)
        _, length = await recv_task
        assert length == size
        np.testing.assert_array_equal(send_buf, recv_buf)


async def test_evaluate_perf(port):
    client = Client()
    server = Server()
    server.listen("127.0.0.1", port)
    await client.aconnect("127.0.0.1", port)

    msg_size = [1, 1024, 1024 * 1024, 1024 * 1024 * 50, 1024 * 1024 * 1024]
    for msg in msg_size:
        t = client.evaluate_perf(msg)
        assert t > 0

    await client.aclose()
    await server.aclose()


# # ==============================================================================
# # State Management and Error Handling Tests
# # ==============================================================================


async def test_client_op_before_connect():
    client = Client()
    buf = np.zeros(1, dtype=np.uint8)
    with pytest.raises(Exception):
        await client.asend(buf, 0)
    with pytest.raises(Exception):
        await client.arecv(buf, 0, 0)
    with pytest.raises(Exception):
        await client.aclose()


async def test_server_op_before_listen():
    server = Server()
    buf = np.zeros(1, dtype=np.uint8)
    with pytest.raises(Exception):
        await server.arecv(buf, 0, 0)
    with pytest.raises(Exception):
        await server.aclose()


async def test_double_connect_or_listen(port):
    server = Server()
    server.listen(SERVER_ADDR, port)
    with pytest.raises(Exception):
        server.listen(SERVER_ADDR, port)

    client = Client()
    await client.aconnect(SERVER_ADDR, port)
    with pytest.raises(Exception):
        await client.aconnect(SERVER_ADDR, port)

    await client.aclose()
    await server.aclose()


async def test_double_close(port):
    client = Client()
    server = Server()
    server.listen("127.0.0.1", port)
    await client.aconnect("127.0.0.1", port)
    await client.aclose()
    await server.aclose()
    # Second close should raise an exception as the object is not in a running state
    with pytest.raises(RuntimeError):
        await client.aclose()
    with pytest.raises(RuntimeError):
        await server.aclose()


async def test_connect_to_dead_server(port):
    client = Client()
    with pytest.raises(Exception) as e_info:
        await asyncio.wait_for(client.aconnect(SERVER_ADDR, port), timeout=5)
    assert "not connected" in str(e_info.value) or "TIMEOUT" in str()


# # ==============================================================================
# # Concurrency and Stress Tests
# # ==============================================================================


async def test_multiple_clients(port):
    server = Server()
    server.listen(SERVER_ADDR, port)
    await asyncio.sleep(0.1)

    num_clients = 5
    clients = [Client() for _ in range(num_clients)]
    connect_tasks = [c.aconnect(SERVER_ADDR, port) for c in clients]
    await asyncio.gather(*connect_tasks)

    await asyncio.sleep(0.2)
    assert len(server.list_clients()) == num_clients

    send_tasks = [
        c.asend(np.array([i], dtype=np.uint8), i) for i, c in enumerate(clients)
    ]
    await asyncio.gather(*send_tasks)

    recv_buf = np.zeros(1, dtype=np.uint8)
    recv_tags = set()
    for _ in range(num_clients):
        tag, _ = await server.arecv(recv_buf, 0, 0)
        recv_tags.add(tag)

    assert recv_tags == set(range(num_clients))

    close_tasks = [c.aclose() for c in clients]
    await asyncio.gather(*close_tasks)
    await server.aclose()


async def test_concurrent_send_recv(port):
    async with gen_server_client(port) as server_and_client:
        server, client = server_and_client
        num_messages = 50

        sends = [client.asend(np.array([i]), i) for i in range(num_messages)]
        recvs = [
            server.arecv(np.zeros(1, dtype=np.uint8), 0, 0) for _ in range(num_messages)
        ]

        results = await asyncio.gather(*sends, *recvs)

        received_tags = {res[0] for res in results if isinstance(res, tuple)}
        assert received_tags == set(range(num_messages))


async def test_bidirectional_traffic(port):
    async with gen_server_client(port) as server_and_client:
        server, client = server_and_client
        client_list = server.list_clients()
        assert len(client_list) > 0
        client_ep = client_list.pop()
        num_messages = 2000

        server_sends = [
            server.asend(client_ep, np.array([i]), 100 + i) for i in range(num_messages)
        ]
        client_recvs = [
            client.arecv(np.zeros(1, dtype=np.uint8), 0, 0) for _ in range(num_messages)
        ]

        client_sends = [
            client.asend(np.array([i]), 200 + i) for i in range(num_messages)
        ]
        server_recvs = [
            server.arecv(np.zeros(1, dtype=np.uint8), 0, 0) for _ in range(num_messages)
        ]

        results = await asyncio.gather(
            *server_sends, *client_recvs, *client_sends, *server_recvs
        )

        client_recv_results = results[num_messages : 2 * num_messages]
        server_recv_results = results[3 * num_messages :]

        client_received_tags = {
            res[0] for res in client_recv_results if res is not None
        }
        server_received_tags = {
            res[0] for res in server_recv_results if res is not None
        }

        assert client_received_tags == set(range(100, 100 + num_messages))
        assert server_received_tags == set(range(200, 200 + num_messages))


async def test_rapid_connect_close_client(port):
    server = Server()
    server.listen(SERVER_ADDR, port)

    num_cycles = 10
    buf = np.zeros(1, dtype=np.uint8)
    buf2 = np.zeros(1, dtype=np.uint8)

    async def once():
        client = Client()
        await client.aconnect(SERVER_ADDR, port)
        await client.asend(buf, 1)
        await client.aclose()

    await asyncio.gather(
        *[once() for i in range(num_cycles)],
        *[server.arecv(buf2, 0, 0) for i in range(num_cycles)],
    )


# # ==============================================================================
# # Resource Management and Lifetime Tests
# # ==============================================================================


async def test_shutdown_with_in_flight_ops(port):
    server = Server()
    server.listen(SERVER_ADDR, port)

    client = Client()
    await client.aconnect(SERVER_ADDR, port)

    recv_buf = np.ones(1024 * 1024 * 1024, dtype=np.uint8)

    # Start recv operations that will never be fulfilled
    async def safe():
        try:
            await client.arecv(recv_buf, 999, 0)
            print("done")
        except Exception as e:
            print(
                "Exception!",
                e,
            )
            assert "cancel" in str(e)

    future = asyncio.create_task(safe())
    await asyncio.sleep(0.01)
    await client.aclose()
    await future
    await server.aclose()


async def test_implicit_destruction_without_close(port):
    # This test is crucial for ensuring the C++ destructors are robust.

    server = Server()
    server.listen(SERVER_ADDR, port)

    client = Client()
    await client.aconnect(SERVER_ADDR, port)

    # Now, `client` and `server` go out of scope.
    del server
    del client

    # Force garbage collection to run
    gc.collect()
    # Give ample time for C++ destructors and their background threads to join.
    # A failure here would likely manifest as a hang or a segfault.
    await asyncio.sleep(0.5)

    # The test passes if it completes without hanging or crashing.
    assert True

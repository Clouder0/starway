import asyncio
import random
import time

import numpy as np

from starway import Client, Server


port = random.randint(10000, 50000)
server = Server()
server.listen("127.0.0.1", port)
print("done")

# future = client.aconnect("127.0.0.1", port, loop)
# time.sleep(0.2)
# async def main():
#     loop = asyncio.get_running_loop()
#     port = random.randint(10000, 50000)
#     server = Server()
#     client_connected = asyncio.Event()
#     server.set_accept_cb(lambda x: client_connected.set())
#     server.listen("127.0.0.1", port)
#     await asyncio.sleep(0.01)
#     client = Client()
#     f_conn = client.aconnect("127.0.0.1", port)
#     await f_conn
#     await client_connected.wait()
#     some_client = None
#     for x in server.list_clients():
#         some_client = x
#         print(x.name)
#         for device_name, port_name in x.view_transports():
#             print("device", device_name, "port", port_name)
#     assert some_client is not None

#     send_buf_c = np.arange(10, dtype=np.uint8) + 1
#     send_buf_s = np.arange(10, dtype=np.uint8) + 2
#     recv_buf_c = np.arange(10, dtype=np.uint8) + 3
#     recv_buf_s = np.arange(10, dtype=np.uint8) + 4
#     f_send_s = server.asend(some_client, send_buf_s, 1, loop=loop)
#     # f_recv_s = server.arecv(recv_buf_s, 2, 0xFF, loop=loop)
#     # f_send_c = client.asend(send_buf_c, 2, loop=loop)
#     f_recv_c = client.arecv(recv_buf_c, 1, 0xFF, loop=loop)
#     time.sleep(0.1)
#     # await asyncio.gather(f_send_s, f_recv_s, f_send_c, f_recv_c)
#     # check results
#     # assert np.all(send_buf_c == recv_buf_s)
#     assert np.all(send_buf_s == recv_buf_c)
#     time.sleep(1)
#     await f_send_s
#     await f_recv_c
#     await client.aclose()
#     await server.aclose()


# asyncio.run(main())

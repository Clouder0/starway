import asyncio
import random
import time

import numpy as np

from starway import Client, Server

def  main():
    loop = asyncio.get_event_loop_policy().get_event_loop()
    port = random.randint(10000, 50000)
    client = Client()
    server = Server()
    server.listen("127.0.0.1", port)
    future = client.aconnect("127.0.0.1", port, loop)
    time.sleep(0.1)
    send_buf = np.arange(10, dtype=np.uint8)


    def done():
        print("Client Done.")


    def fail(reason):
        print("Client Failed:", reason)


    def sdone(sender_tag, length):
        print("Server Done.", sender_tag, length)


    def sfail(reason):
        print("Server Failed.", reason)


    recv_buf = np.empty(10, np.uint8)
    client.send(send_buf, 0, done, fail)
    server.recv(recv_buf, 0, 0, sdone, sfail)

    time.sleep(0.1)

    client.recv(recv_buf, 0, 0, sdone, sfail)
    server.send(next(iter(server.list_clients())), send_buf, 0, done, fail)
    time.sleep(0.1)
    client.aclose(loop)
    server.aclose(loop)
    time.sleep(0.1)

main()

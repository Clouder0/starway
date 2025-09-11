import asyncio
import random

import numpy as np

from starway import Client, Server

SERVER_ADDR = "127.0.0.1"
port = random.randint(10000, 50000)


async def test_main():
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


asyncio.run(test_main())

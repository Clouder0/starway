import asyncio

import pytest


@pytest.mark.asyncio
async def test_agent_sends_hello_and_accepts_ack() -> None:
    from bench.cluster.agent import run_agent
    from bench.cluster.protocol import read_jsonl, write_jsonl

    got: list[dict] = []

    async def handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            got.append(await read_jsonl(reader))
            await write_jsonl(writer, {"type": "hello_ack", "ok": True})
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, host="127.0.0.1", port=0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        await run_agent(
            agent_id="hostA",
            coordinator_host=str(host),
            coordinator_port=int(port),
            advertise_host="127.0.0.1",
            stop_after_handshake=True,
        )

        assert len(got) == 1
        assert got[0]["type"] == "hello"
        assert got[0]["agent_id"] == "hostA"
        assert got[0]["advertise_host"] == "127.0.0.1"
    finally:
        server.close()
        await server.wait_closed()

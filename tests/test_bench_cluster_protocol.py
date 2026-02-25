import asyncio

import pytest


def test_encode_message_appends_newline() -> None:
    from bench.cluster.protocol import encode_message

    out = encode_message({"type": "hello", "agent_id": "a"})
    assert out.endswith(b"\n")


def test_decode_message_validates_dict_and_type() -> None:
    from bench.cluster.protocol import decode_message

    msg = decode_message(b'{"type":"hello"}\n')
    assert msg["type"] == "hello"

    with pytest.raises(ValueError, match=r"must be a JSON object"):
        decode_message(b"[]\n")

    with pytest.raises(ValueError, match=r"type"):
        decode_message(b"{}\n")

    with pytest.raises(ValueError, match=r"type"):
        decode_message(b'{"type": 123}\n')


@pytest.mark.asyncio
async def test_jsonl_roundtrip_over_stream() -> None:
    from bench.cluster.protocol import read_jsonl, write_jsonl

    got: list[dict] = []

    async def handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            got.append(await read_jsonl(reader))
            await write_jsonl(writer, {"type": "ack", "ok": True})
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, host="127.0.0.1", port=0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        try:
            await write_jsonl(writer, {"type": "hello"})
            resp = await read_jsonl(reader)
            assert resp == {"ok": True, "type": "ack"}
        finally:
            writer.close()
            await writer.wait_closed()

        assert got == [{"type": "hello"}]
    finally:
        server.close()
        await server.wait_closed()

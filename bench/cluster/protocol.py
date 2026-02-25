from __future__ import annotations

import asyncio
import json
from typing import Any


def _validate_message(msg: Any) -> dict[str, Any]:
    if not isinstance(msg, dict):
        raise ValueError("message must be a JSON object")
    typ = msg.get("type")
    if not isinstance(typ, str) or not typ.strip():
        raise ValueError("message.type must be a non-empty string")
    return msg


def encode_message(msg: dict[str, Any]) -> bytes:
    """Encode a JSONL message (single JSON object + newline)."""

    _validate_message(msg)
    payload = json.dumps(
        msg,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return payload + b"\n"


def decode_message(line: bytes) -> dict[str, Any]:
    """Decode one JSONL line into a validated dict."""

    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("message must be valid UTF-8") from exc

    text = text.strip()
    try:
        msg = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("message must be valid JSON") from exc
    if not isinstance(msg, dict):
        raise ValueError("message must be a JSON object")
    return _validate_message(msg)


async def read_jsonl(reader: asyncio.StreamReader) -> dict[str, Any]:
    """Read exactly one JSONL message from a stream."""

    line = await reader.readline()
    if line == b"":
        raise EOFError("EOF while reading JSONL message")
    return decode_message(line)


async def write_jsonl(writer: asyncio.StreamWriter, msg: dict[str, Any]) -> None:
    """Write exactly one JSONL message to a stream."""

    writer.write(encode_message(msg))
    await writer.drain()

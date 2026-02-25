from __future__ import annotations

import asyncio
import ipaddress
from dataclasses import dataclass
from typing import Any

from bench.cluster.protocol import read_jsonl, write_jsonl


def _require_non_empty_str(value: Any, *, ctx: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{ctx} must be a non-empty string")
    return str(value)


def _require_ipv4(value: Any, *, ctx: str) -> str:
    host = _require_non_empty_str(value, ctx=ctx)
    try:
        ipaddress.IPv4Address(host)
    except Exception as exc:
        raise ValueError(f"{ctx} must be an IPv4 address") from exc
    return host


@dataclass
class AgentConn:
    agent_id: str
    advertise_host: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def send(self, msg: dict[str, Any]) -> None:
        await write_jsonl(self.writer, msg)

    async def recv(self) -> dict[str, Any]:
        return await read_jsonl(self.reader)

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


class CoordinatorServer:
    def __init__(
        self,
        *,
        bind_host: str,
        port: int,
        expected_agent_ids: tuple[str, ...],
        port_range_min: int,
        port_range_max: int,
    ) -> None:
        self._bind_host = _require_ipv4(bind_host, ctx="cluster.coordinator_bind_host")
        if not (1 <= int(port) <= 65535):
            raise ValueError("cluster.coordinator_port must be in [1, 65535]")
        self._port = int(port)
        self._expected_agent_ids = tuple(str(a) for a in expected_agent_ids)
        self._port_range_min = int(port_range_min)
        self._port_range_max = int(port_range_max)

        self._server: asyncio.base_events.Server | None = None
        self._agents: dict[str, AgentConn] = {}
        self._agents_ready = asyncio.Event()

    @property
    def bind_host(self) -> str:
        return self._bind_host

    @property
    def port(self) -> int:
        return self._port

    def agent(self, agent_id: str) -> AgentConn:
        return self._agents[str(agent_id)]

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("CoordinatorServer already started")
        self._server = await asyncio.start_server(
            self._handle_conn,
            host=self._bind_host,
            port=self._port,
        )

    async def wait_for_agents(self, *, timeout_s: float) -> None:
        expected = set(self._expected_agent_ids)

        async with asyncio.timeout(timeout_s):
            while True:
                if expected.issubset(set(self._agents.keys())):
                    return
                await self._agents_ready.wait()
                # Reset so we can wait for the next arrival.
                self._agents_ready.clear()

    async def close(self) -> None:
        for conn in list(self._agents.values()):
            try:
                await conn.close()
            except Exception:
                pass
        self._agents.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_conn(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            hello = await read_jsonl(reader)
            if hello.get("type") != "hello":
                raise ValueError(f"expected hello, got: {hello}")

            agent_id = _require_non_empty_str(
                hello.get("agent_id"), ctx="hello.agent_id"
            )
            advertise_host = _require_ipv4(
                hello.get("advertise_host"), ctx="hello.advertise_host"
            )

            if agent_id not in self._expected_agent_ids:
                await write_jsonl(
                    writer,
                    {
                        "type": "hello_ack",
                        "ok": False,
                        "error": f"unexpected agent_id: {agent_id}",
                    },
                )
                return
            if agent_id in self._agents:
                await write_jsonl(
                    writer,
                    {
                        "type": "hello_ack",
                        "ok": False,
                        "error": f"duplicate agent_id: {agent_id}",
                    },
                )
                return

            self._agents[agent_id] = AgentConn(
                agent_id=agent_id,
                advertise_host=advertise_host,
                reader=reader,
                writer=writer,
            )

            await write_jsonl(
                writer,
                {
                    "type": "hello_ack",
                    "ok": True,
                    "port_range_min": self._port_range_min,
                    "port_range_max": self._port_range_max,
                },
            )
            self._agents_ready.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Best-effort close on protocol errors.
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

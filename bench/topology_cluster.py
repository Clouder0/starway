from __future__ import annotations

import asyncio
from typing import Any

from bench.cluster.coordinator import CoordinatorServer
from bench.types import ClusterConfig


class ClusterSession:
    def __init__(self, *, cluster: ClusterConfig) -> None:
        self._cluster = cluster
        self._coord = CoordinatorServer(
            bind_host=cluster.coordinator_bind_host,
            port=int(cluster.coordinator_port),
            expected_agent_ids=(cluster.server_agent_id, cluster.client_agent_id),
            port_range_min=int(cluster.port_range_min),
            port_range_max=int(cluster.port_range_max),
        )

    async def __aenter__(self) -> "ClusterSession":
        await self._coord.start()
        await self._coord.wait_for_agents(
            timeout_s=float(self._cluster.agent_connect_timeout_s)
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._coord.close()

    async def run_point(
        self,
        *,
        point_id: str,
        point_spec: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Run one point across the two agents.

        Returns:
        - connect info (host/port(s)) used by the client
        - result dict from the client role (metrics, warnings, ...)
        """

        server = self._coord.agent(self._cluster.server_agent_id)
        client = self._coord.agent(self._cluster.client_agent_id)

        async def recv_for(conn, *, want_types: set[str]) -> dict[str, Any]:
            # A timed-out point may still deliver late messages. Filter by point_id
            # so we can safely continue to the next point.
            while True:
                msg = await conn.recv()
                if msg.get("point_id") != point_id:
                    continue
                typ = msg.get("type")
                if typ in want_types:
                    return msg
                raise RuntimeError(f"unexpected message: {msg}")

        await server.send({"type": "run_server", "point_id": point_id, **point_spec})
        server_msg = await recv_for(server, want_types={"server_ready", "server_done"})
        if server_msg.get("type") != "server_ready":
            raise RuntimeError(str(server_msg.get("error") or "server failed"))
        connect = server_msg.get("connect")
        if not isinstance(connect, dict):
            raise RuntimeError(f"server_ready.connect must be an object: {server_msg}")

        await client.send(
            {
                "type": "run_client",
                "point_id": point_id,
                "connect": connect,
                **point_spec,
            }
        )

        client_t = asyncio.create_task(recv_for(client, want_types={"point_result"}))
        server_done_t = asyncio.create_task(
            recv_for(server, want_types={"server_done"})
        )
        client_msg, server_done = await asyncio.gather(client_t, server_done_t)

        if not bool(client_msg.get("ok", False)):
            raise RuntimeError(str(client_msg.get("error") or "client point failed"))
        if not bool(server_done.get("ok", False)):
            raise RuntimeError(str(server_done.get("error") or "server failed"))

        result = client_msg.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"point_result.result must be an object: {client_msg}")
        return dict(connect), dict(result)

    async def cancel_point(self, *, point_id: str) -> None:
        """Best-effort cancellation of an in-flight point on both agents."""

        server = self._coord.agent(self._cluster.server_agent_id)
        client = self._coord.agent(self._cluster.client_agent_id)

        await server.send({"type": "cancel_point", "point_id": str(point_id)})
        await client.send({"type": "cancel_point", "point_id": str(point_id)})

        async def wait_ack(conn) -> None:
            while True:
                msg = await conn.recv()
                if msg.get("type") != "cancel_ack":
                    continue
                if msg.get("point_id") != point_id:
                    continue
                return

        timeout_s = float(self._cluster.agent_rpc_timeout_s)
        async with asyncio.timeout(timeout_s):
            await asyncio.gather(wait_ack(server), wait_ack(client))

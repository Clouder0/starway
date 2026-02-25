from __future__ import annotations

import argparse
import asyncio
import ipaddress
import multiprocessing
import traceback
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from typing import Any

from bench.cluster.protocol import read_jsonl, write_jsonl
from bench.cluster.ports import PortAllocator


def _require_ipv4(host: str) -> str:
    try:
        ipaddress.IPv4Address(host)
    except Exception as exc:
        raise ValueError(f"expected IPv4 address, got: {host!r}") from exc
    return host


def _parse_ip_port(value: str) -> tuple[str, int]:
    if ":" not in value:
        raise ValueError("expected ip:port")
    host_txt, port_txt = value.rsplit(":", 1)
    host = _require_ipv4(host_txt)
    try:
        port = int(port_txt)
    except Exception as exc:
        raise ValueError("expected ip:port") from exc
    if not (1 <= port <= 65535):
        raise ValueError("port must be in [1, 65535]")
    return host, int(port)


@dataclass
class _ActiveRun:
    point_id: str
    kind: str  # "server" or "client"
    cancel: asyncio.Event
    proc: BaseProcess | None = None
    task: asyncio.Task[None] | None = None


async def run_agent(
    *,
    agent_id: str,
    coordinator_host: str,
    coordinator_port: int,
    advertise_host: str,
    stop_after_handshake: bool = False,
) -> None:
    """Run a cluster agent.

    The agent connects to the coordinator via TCP and exchanges JSONL messages.
    """

    _require_ipv4(coordinator_host)
    if not (1 <= int(coordinator_port) <= 65535):
        raise ValueError("coordinator_port must be in [1, 65535]")
    _require_ipv4(advertise_host)
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise ValueError("agent_id must be a non-empty string")

    reader, writer = await asyncio.open_connection(
        host=str(coordinator_host), port=int(coordinator_port)
    )
    try:
        await write_jsonl(
            writer,
            {
                "type": "hello",
                "agent_id": str(agent_id),
                "advertise_host": str(advertise_host),
                "capabilities": ["asyncio_tcp", "starway_tcp"],
            },
        )
        ack = await read_jsonl(reader)
        if ack.get("type") != "hello_ack" or not bool(ack.get("ok", False)):
            raise RuntimeError(f"handshake failed: {ack}")
        if stop_after_handshake:
            return

        port_range_min = int(ack.get("port_range_min", 0))
        port_range_max = int(ack.get("port_range_max", 0))
        if port_range_min <= 0 or port_range_max <= 0:
            raise RuntimeError(f"hello_ack missing port range: {ack}")
        port_alloc = PortAllocator(port_min=port_range_min, port_max=port_range_max)

        mp = multiprocessing.get_context("spawn")

        write_lock = asyncio.Lock()

        async def send(msg: dict[str, Any]) -> None:
            async with write_lock:
                await write_jsonl(writer, msg)

        active: _ActiveRun | None = None

        async def recv_pipe(conn, *, timeout_s: float) -> Any:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + float(timeout_s)
            while True:
                if conn.poll(0.01):
                    return conn.recv()
                if loop.time() >= deadline:
                    raise TimeoutError("Timed out waiting for worker message")
                await asyncio.sleep(0)

        async def handle_run_server(run: _ActiveRun, msg: dict[str, Any]) -> None:
            nonlocal active

            point_id = str(msg.get("point_id", ""))
            comparator_type = str(msg.get("comparator_type", ""))
            comparator_env = msg.get("comparator_env", {})
            comparator_options = msg.get("comparator_options", {})
            workload = str(msg.get("workload", ""))
            params = msg.get("params", {})
            cpu_affinity = tuple(int(v) for v in msg.get("cpu_affinity", []) or [])
            payload_verify = str(msg.get("payload_verify", "none"))

            if not point_id:
                raise ValueError("run_server.point_id is required")
            if point_id != run.point_id:
                raise ValueError("run_server.point_id does not match active run")
            if comparator_type not in ("asyncio_tcp", "starway_tcp"):
                raise NotImplementedError(
                    "cluster agent supports only asyncio_tcp and starway_tcp "
                    f"for now (got {comparator_type})"
                )
            if not isinstance(comparator_env, dict):
                raise ValueError("run_server.comparator_env must be a mapping")
            if not isinstance(comparator_options, dict):
                raise ValueError("run_server.comparator_options must be a mapping")
            if not isinstance(params, dict):
                raise ValueError("run_server.params must be a mapping")

            preflight_seed = int(params.get("seed", 0))

            server_recv, server_send = mp.Pipe(duplex=False)
            proc = None
            try:
                if run.cancel.is_set():
                    return
                if workload == "control_under_load":
                    if comparator_type == "asyncio_tcp":
                        from bench.topology_2proc import (
                            _server_worker_asyncio_tcp_control_under_load,
                        )

                        server_target = _server_worker_asyncio_tcp_control_under_load
                    else:
                        from bench.topology_2proc import (
                            _server_worker_starway_tcp_control_under_load,
                        )

                        server_target = _server_worker_starway_tcp_control_under_load

                    data_port, control_port = port_alloc.allocate(count=2)
                    proc = mp.Process(
                        target=server_target,
                        kwargs={
                            "send_conn": server_send,
                            "comparator_env": dict(comparator_env),
                            "comparator_options": dict(comparator_options),
                            "params": dict(params),
                            "cpu_affinity": tuple(cpu_affinity),
                            "payload_verify": payload_verify,
                            "data_preflight_sizes": (),
                            "control_preflight_sizes": (),
                            "preflight_seed": preflight_seed,
                            "bind_host": advertise_host,
                            "data_bind_port": int(data_port),
                            "control_bind_port": int(control_port),
                        },
                        daemon=True,
                    )
                    proc.start()
                    run.proc = proc
                    listen_msg = await recv_pipe(server_recv, timeout_s=5.0)
                    if listen_msg.get("type") == "error":
                        raise RuntimeError(str(listen_msg.get("error")))
                    if listen_msg.get("type") != "listen2":
                        raise RuntimeError(f"unexpected listen message: {listen_msg}")
                    connect = {
                        "host": str(listen_msg["host"]),
                        "data_port": int(listen_msg["data_port"]),
                        "control_port": int(listen_msg["control_port"]),
                    }
                else:
                    if comparator_type == "asyncio_tcp":
                        from bench.topology_2proc import _server_worker_asyncio_tcp

                        server_target = _server_worker_asyncio_tcp
                    else:
                        from bench.topology_2proc import _server_worker_starway_tcp

                        server_target = _server_worker_starway_tcp

                    (port,) = port_alloc.allocate(count=1)
                    proc = mp.Process(
                        target=server_target,
                        kwargs={
                            "send_conn": server_send,
                            "comparator_env": dict(comparator_env),
                            "comparator_options": dict(comparator_options),
                            "workload": workload,
                            "params": dict(params),
                            "cpu_affinity": tuple(cpu_affinity),
                            "payload_verify": payload_verify,
                            "preflight_sizes": (),
                            "preflight_seed": preflight_seed,
                            "bind_host": advertise_host,
                            "bind_port": int(port),
                        },
                        daemon=True,
                    )
                    proc.start()
                    run.proc = proc
                    listen_msg = await recv_pipe(server_recv, timeout_s=5.0)
                    if listen_msg.get("type") == "error":
                        raise RuntimeError(str(listen_msg.get("error")))
                    if listen_msg.get("type") != "listen":
                        raise RuntimeError(f"unexpected listen message: {listen_msg}")
                    connect = {
                        "host": str(listen_msg["host"]),
                        "port": int(listen_msg["port"]),
                    }

                if run.cancel.is_set():
                    return
                await send(
                    {"type": "server_ready", "point_id": point_id, "connect": connect}
                )

                done_msg = await recv_pipe(server_recv, timeout_s=60.0)
                if done_msg.get("type") == "error":
                    raise RuntimeError(str(done_msg.get("error")))
                if done_msg.get("type") != "done":
                    raise RuntimeError(f"unexpected server message: {done_msg}")

                if run.cancel.is_set():
                    return
                await send({"type": "server_done", "point_id": point_id, "ok": True})
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not run.cancel.is_set():
                    try:
                        await send(
                            {
                                "type": "server_done",
                                "point_id": point_id,
                                "ok": False,
                                "error": str(exc),
                                "traceback": traceback.format_exc(),
                            }
                        )
                    except Exception:
                        pass
            finally:
                if proc is not None:
                    if proc.is_alive():
                        proc.terminate()
                    proc.join(timeout=2.0)

                if active is run:
                    active = None

        async def handle_run_client(run: _ActiveRun, msg: dict[str, Any]) -> None:
            nonlocal active

            point_id = str(msg.get("point_id", ""))
            comparator_type = str(msg.get("comparator_type", ""))
            comparator_env = msg.get("comparator_env", {})
            comparator_options = msg.get("comparator_options", {})
            workload = str(msg.get("workload", ""))
            params = msg.get("params", {})
            cpu_affinity = tuple(int(v) for v in msg.get("cpu_affinity", []) or [])
            payload_verify = str(msg.get("payload_verify", "none"))
            connect = msg.get("connect")

            if not point_id:
                raise ValueError("run_client.point_id is required")
            if point_id != run.point_id:
                raise ValueError("run_client.point_id does not match active run")
            if comparator_type not in ("asyncio_tcp", "starway_tcp"):
                raise NotImplementedError(
                    "cluster agent supports only asyncio_tcp and starway_tcp "
                    f"for now (got {comparator_type})"
                )
            if not isinstance(comparator_env, dict):
                raise ValueError("run_client.comparator_env must be a mapping")
            if not isinstance(comparator_options, dict):
                raise ValueError("run_client.comparator_options must be a mapping")
            if not isinstance(params, dict):
                raise ValueError("run_client.params must be a mapping")
            if not isinstance(connect, dict):
                raise ValueError("run_client.connect must be an object")

            preflight_seed = int(params.get("seed", 0))

            client_recv, client_send = mp.Pipe(duplex=False)
            proc = None
            try:
                if run.cancel.is_set():
                    return
                if workload == "control_under_load":
                    if comparator_type == "asyncio_tcp":
                        from bench.topology_2proc import (
                            _client_worker_asyncio_tcp_control_under_load,
                        )

                        client_target = _client_worker_asyncio_tcp_control_under_load
                    else:
                        from bench.topology_2proc import (
                            _client_worker_starway_tcp_control_under_load,
                        )

                        client_target = _client_worker_starway_tcp_control_under_load

                    proc = mp.Process(
                        target=client_target,
                        kwargs={
                            "send_conn": client_send,
                            "comparator_env": dict(comparator_env),
                            "host": str(connect["host"]),
                            "data_port": int(connect["data_port"]),
                            "control_port": int(connect["control_port"]),
                            "comparator_options": dict(comparator_options),
                            "params": dict(params),
                            "cpu_affinity": tuple(cpu_affinity),
                            "payload_verify": payload_verify,
                            "data_preflight_sizes": (),
                            "control_preflight_sizes": (),
                            "preflight_seed": preflight_seed,
                        },
                        daemon=True,
                    )
                else:
                    if comparator_type == "asyncio_tcp":
                        from bench.topology_2proc import _client_worker_asyncio_tcp

                        client_target = _client_worker_asyncio_tcp
                    else:
                        from bench.topology_2proc import _client_worker_starway_tcp

                        client_target = _client_worker_starway_tcp

                    proc = mp.Process(
                        target=client_target,
                        kwargs={
                            "send_conn": client_send,
                            "comparator_env": dict(comparator_env),
                            "host": str(connect["host"]),
                            "port": int(connect["port"]),
                            "comparator_options": dict(comparator_options),
                            "workload": workload,
                            "params": dict(params),
                            "cpu_affinity": tuple(cpu_affinity),
                            "payload_verify": payload_verify,
                            "preflight_sizes": (),
                            "preflight_seed": preflight_seed,
                        },
                        daemon=True,
                    )

                proc.start()
                run.proc = proc
                client_msg = await recv_pipe(client_recv, timeout_s=60.0)
                if client_msg.get("type") != "result":
                    raise RuntimeError(f"unexpected client message: {client_msg}")
                if not bool(client_msg.get("ok", False)):
                    raise RuntimeError(str(client_msg.get("error")))

                result = client_msg.get("result")
                if not isinstance(result, dict):
                    raise RuntimeError(f"client result must be an object: {client_msg}")

                if run.cancel.is_set():
                    return
                await send(
                    {
                        "type": "point_result",
                        "point_id": point_id,
                        "ok": True,
                        "result": result,
                    }
                )
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not run.cancel.is_set():
                    try:
                        await send(
                            {
                                "type": "point_result",
                                "point_id": point_id,
                                "ok": False,
                                "error": str(exc),
                                "traceback": traceback.format_exc(),
                            }
                        )
                    except Exception:
                        pass
            finally:
                if proc is not None:
                    if proc.is_alive():
                        proc.terminate()
                    proc.join(timeout=2.0)

                if active is run:
                    active = None

        while True:
            try:
                msg = await read_jsonl(reader)
            except EOFError:
                return

            typ = str(msg.get("type", ""))
            if typ == "run_server":
                point_id = str(msg.get("point_id", ""))
                if active is not None:
                    await send(
                        {
                            "type": "server_done",
                            "point_id": point_id,
                            "ok": False,
                            "error": "agent busy",
                        }
                    )
                    continue

                run = _ActiveRun(
                    point_id=point_id,
                    kind="server",
                    cancel=asyncio.Event(),
                )
                active = run
                run.task = asyncio.create_task(handle_run_server(run, msg))
            elif typ == "run_client":
                point_id = str(msg.get("point_id", ""))
                if active is not None:
                    await send(
                        {
                            "type": "point_result",
                            "point_id": point_id,
                            "ok": False,
                            "error": "agent busy",
                        }
                    )
                    continue

                run = _ActiveRun(
                    point_id=point_id,
                    kind="client",
                    cancel=asyncio.Event(),
                )
                active = run
                run.task = asyncio.create_task(handle_run_client(run, msg))
            elif typ == "cancel_point":
                point_id = str(msg.get("point_id", ""))
                if active is not None and active.point_id == point_id:
                    active.cancel.set()
                    if active.proc is not None and active.proc.is_alive():
                        active.proc.terminate()
                        active.proc.join(timeout=2.0)
                    if active.task is not None:
                        active.task.cancel()
                        try:
                            await active.task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    active = None

                await send({"type": "cancel_ack", "point_id": point_id, "ok": True})
            else:
                raise ValueError(f"unknown message type: {typ!r}")

            if active is not None and active.task is not None and active.task.done():
                active = None
    finally:
        writer.close()
        await writer.wait_closed()


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Starway bench cluster agent")
    parser.add_argument("--agent-id", required=True)
    parser.add_argument(
        "--coordinator",
        required=True,
        help="Coordinator address in IPv4:port form",
    )
    parser.add_argument(
        "--advertise-host",
        required=True,
        help="IPv4 address other host should connect to",
    )
    args = parser.parse_args(argv)

    coord_host, coord_port = _parse_ip_port(str(args.coordinator))
    asyncio.run(
        run_agent(
            agent_id=str(args.agent_id),
            coordinator_host=coord_host,
            coordinator_port=int(coord_port),
            advertise_host=_require_ipv4(str(args.advertise_host)),
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

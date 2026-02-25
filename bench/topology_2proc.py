from __future__ import annotations

import asyncio
import contextlib
import os
import traceback
from collections.abc import Callable
from typing import Any, Literal

import multiprocessing


PayloadVerifyMode = Literal["none", "preflight", "full"]


def _apply_env(env: dict[str, str]) -> None:
    for key, value in env.items():
        existing = os.environ.get(key)
        if existing is None:
            os.environ[key] = value
        elif existing != value:
            raise RuntimeError(
                f"Comparator env conflict for {key}: '{existing}' != '{value}'. "
                "Env must be consistent within one worker process."
            )


def _apply_cpu_affinity(cpus: tuple[int, ...] | None) -> None:
    if not cpus:
        return
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, set(int(c) for c in cpus))


def _load_roles(workload: str) -> tuple[Callable[..., Any], Callable[..., Any]]:
    import bench.workloads as workloads

    client_name = f"{workload}_client"
    server_name = f"{workload}_server"
    try:
        client_role = getattr(workloads, client_name)
        server_role = getattr(workloads, server_name)
    except AttributeError as exc:
        raise KeyError(f"Workload roles not found for '{workload}'") from exc
    return client_role, server_role


async def _recv_with_timeout(conn, *, timeout_s: float) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + float(timeout_s)
    while True:
        if conn.poll(0.01):
            return conn.recv()
        if loop.time() >= deadline:
            raise TimeoutError("Timed out waiting for worker message")
        await asyncio.sleep(0)


def _align_up(n: int, align: int) -> int:
    if align <= 0:
        raise ValueError("align must be > 0")
    return ((int(n) + align - 1) // align) * align


def _parse_bytes(value: Any) -> int:
    from bench.util import parse_size

    if isinstance(value, str):
        return int(parse_size(value))
    return int(value)


def _max_payload_bytes_for_workload(workload: str, params: dict[str, Any]) -> int:
    if workload == "control_rtt":
        return _parse_bytes(params.get("msg_bytes", 64))
    if workload == "small_message_pressure":
        return _parse_bytes(params.get("msg_bytes", 1024))
    if workload == "bulk_stream":
        return _parse_bytes(params.get("chunk_bytes", "4MiB"))
    if workload == "mixed_size":
        sizes = params.get("sizes", ["64B", "1KiB", "64KiB"])
        if not isinstance(sizes, list):
            raise ValueError("mixed_size.params.sizes must be a list")
        return max((_parse_bytes(v) for v in sizes), default=0)
    if workload == "bucket_duplex_step":
        return _parse_bytes(params.get("bucket_bytes", "4MiB"))
    raise KeyError(f"Unknown workload '{workload}'")


def _preflight_sizes_for_workload(
    workload: str, params: dict[str, Any]
) -> tuple[int, ...]:
    sizes: list[int] = [0]
    if workload == "mixed_size":
        raw_sizes = params.get("sizes", ["64B", "1KiB", "64KiB"])
        if not isinstance(raw_sizes, list):
            raise ValueError("mixed_size.params.sizes must be a list")
        sizes.extend(_parse_bytes(v) for v in raw_sizes)
    else:
        max_payload = _max_payload_bytes_for_workload(workload, params)
        if max_payload > 0:
            sizes.extend([1, min(64, max_payload), max_payload])

    # De-dup while preserving order.
    out: list[int] = []
    seen: set[int] = set()
    for s in (int(v) for v in sizes):
        if s < 0:
            raise ValueError("preflight sizes must be >= 0")
        if s not in seen:
            seen.add(s)
            out.append(s)
    return tuple(out)


def _shm_slot_stride(payload_bytes: int) -> int:
    header_bytes = 16
    cacheline_bytes = 64
    return _align_up(header_bytes + int(payload_bytes), cacheline_bytes)


def _choose_shm_slots(
    *,
    slot_payload_bytes: int,
    target_bytes: int,
    min_slots: int,
    max_slots: int,
) -> int:
    stride = _shm_slot_stride(slot_payload_bytes)
    slots = int(target_bytes) // int(stride) if stride > 0 else 0
    slots = max(int(min_slots), slots)
    slots = min(int(max_slots), slots)
    return int(slots)


def _server_worker_asyncio_tcp(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
    bind_host: str = "127.0.0.1",
    bind_port: int = 0,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import socket

        from bench.comparators.asyncio_tcp import AsyncioTcpPeer

        client_role, server_role = _load_roles(workload)

        host = str(bind_host)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(bind_port)))
        sock.listen(1)
        sock.setblocking(False)

        port = int(sock.getsockname()[1])
        send_conn.send({"type": "listen", "host": host, "port": port})

        loop = asyncio.get_running_loop()
        conn, _addr = await loop.sock_accept(sock)
        try:
            conn.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_NODELAY,
                1 if comparator_options.get("tcp_nodelay", True) else 0,
            )
            conn.setblocking(False)
            peer = AsyncioTcpPeer(
                sock=conn,
                loop=loop,
                coalesce_send_bytes=int(
                    comparator_options.get("coalesce_send_bytes", 4096)
                ),
            )
            try:
                if payload_verify == "preflight" and preflight_sizes:
                    from bench.preflight import preflight_verify_peer_server

                    await preflight_verify_peer_server(
                        peer, sizes=preflight_sizes, seed=int(preflight_seed)
                    )
                await server_role(peer, params)
            finally:
                await peer.close()
        finally:
            sock.close()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_asyncio_tcp(
    send_conn,
    *,
    comparator_env: dict[str, str],
    host: str,
    port: int,
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import socket

        from bench.comparators.asyncio_tcp import AsyncioTcpPeer

        client_role, _server_role = _load_roles(workload)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(
            socket.IPPROTO_TCP,
            socket.TCP_NODELAY,
            1 if comparator_options.get("tcp_nodelay", True) else 0,
        )
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        await loop.sock_connect(sock, (host, int(port)))
        peer = AsyncioTcpPeer(
            sock=sock,
            loop=loop,
            coalesce_send_bytes=int(
                comparator_options.get("coalesce_send_bytes", 4096)
            ),
        )
        try:
            if payload_verify == "preflight" and preflight_sizes:
                from bench.preflight import preflight_verify_peer_client

                await preflight_verify_peer_client(
                    peer, sizes=preflight_sizes, seed=int(preflight_seed)
                )
            return await client_role(peer, params)
        finally:
            await peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_asyncio_tcp_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
    bind_host: str = "127.0.0.1",
    data_bind_port: int = 0,
    control_bind_port: int = 0,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import socket

        from bench.comparators.asyncio_tcp import AsyncioTcpPeer
        from bench.workloads import bulk_stream_server, control_rtt_server

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        host = str(bind_host)

        def make_listen_sock(port: int) -> socket.socket:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, int(port)))
            sock.listen(1)
            sock.setblocking(False)
            return sock

        data_sock = make_listen_sock(int(data_bind_port))
        control_sock = make_listen_sock(int(control_bind_port))
        try:
            data_port = int(data_sock.getsockname()[1])
            control_port = int(control_sock.getsockname()[1])
            send_conn.send(
                {
                    "type": "listen2",
                    "host": host,
                    "data_port": data_port,
                    "control_port": control_port,
                }
            )

            loop = asyncio.get_running_loop()
            data_conn, _addr = await loop.sock_accept(data_sock)
            control_conn, _addr2 = await loop.sock_accept(control_sock)
            try:
                nodelay = 1 if comparator_options.get("tcp_nodelay", True) else 0
                for conn in (data_conn, control_conn):
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, nodelay)
                    conn.setblocking(False)

                coalesce = int(comparator_options.get("coalesce_send_bytes", 4096))
                data_peer = AsyncioTcpPeer(
                    sock=data_conn, loop=loop, coalesce_send_bytes=coalesce
                )
                control_peer = AsyncioTcpPeer(
                    sock=control_conn, loop=loop, coalesce_send_bytes=coalesce
                )

                async def run_control_twice() -> None:
                    await control_rtt_server(control_peer, control_params)
                    await control_rtt_server(control_peer, control_params)

                try:
                    if payload_verify == "preflight":
                        from bench.preflight import preflight_verify_peer_server

                        if data_preflight_sizes:
                            await preflight_verify_peer_server(
                                data_peer,
                                sizes=data_preflight_sizes,
                                seed=int(preflight_seed),
                            )
                        if control_preflight_sizes:
                            await preflight_verify_peer_server(
                                control_peer,
                                sizes=control_preflight_sizes,
                                seed=int(preflight_seed),
                            )

                    await asyncio.gather(
                        bulk_stream_server(data_peer, bulk_params),
                        run_control_twice(),
                    )
                finally:
                    await data_peer.close()
                    await control_peer.close()
            finally:
                with contextlib.suppress(Exception):
                    data_conn.close()
                with contextlib.suppress(Exception):
                    control_conn.close()
        finally:
            data_sock.close()
            control_sock.close()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_asyncio_tcp_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    host: str,
    data_port: int,
    control_port: int,
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import socket

        from bench.comparators.asyncio_tcp import AsyncioTcpPeer
        from bench.workloads import bulk_stream_client, control_rtt_client

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        loop = asyncio.get_running_loop()
        nodelay = 1 if comparator_options.get("tcp_nodelay", True) else 0
        coalesce = int(comparator_options.get("coalesce_send_bytes", 4096))

        data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            for sock in (data_sock, control_sock):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, nodelay)
                sock.setblocking(False)

            await loop.sock_connect(data_sock, (host, int(data_port)))
            await loop.sock_connect(control_sock, (host, int(control_port)))

            data_peer = AsyncioTcpPeer(
                sock=data_sock,
                loop=loop,
                coalesce_send_bytes=coalesce,
            )
            control_peer = AsyncioTcpPeer(
                sock=control_sock,
                loop=loop,
                coalesce_send_bytes=coalesce,
            )
            try:
                if payload_verify == "preflight":
                    from bench.preflight import preflight_verify_peer_client

                    if data_preflight_sizes:
                        await preflight_verify_peer_client(
                            data_peer,
                            sizes=data_preflight_sizes,
                            seed=int(preflight_seed),
                        )
                    if control_preflight_sizes:
                        await preflight_verify_peer_client(
                            control_peer,
                            sizes=control_preflight_sizes,
                            seed=int(preflight_seed),
                        )

                idle = await control_rtt_client(control_peer, control_params)
                idle_p99 = float(idle.get("metrics", {}).get("rtt_p99_us", 0.0))

                async with asyncio.TaskGroup() as tg:
                    bulk_t = tg.create_task(bulk_stream_client(data_peer, bulk_params))
                    loaded_t = tg.create_task(
                        control_rtt_client(control_peer, control_params)
                    )

                loaded = loaded_t.result()
                bulk_res = bulk_t.result()

                loaded_p99 = float(loaded.get("metrics", {}).get("rtt_p99_us", 0.0))
                inflation = (loaded_p99 / idle_p99) if idle_p99 > 0 else 0.0

                metrics = {
                    "control_idle_p99_us": idle_p99,
                    "control_loaded_p99_us": loaded_p99,
                    "p99_inflation_ratio": float(inflation),
                    "bulk_goodput_gbps": float(
                        bulk_res.get("metrics", {}).get("goodput_gbps", 0.0)
                    ),
                }

                warnings: list[str] = []
                for sub in (idle, loaded, bulk_res):
                    warnings.extend(list(sub.get("warnings", [])))

                # De-dup while preserving order.
                seen: set[str] = set()
                warnings = [w for w in warnings if not (w in seen or seen.add(w))]

                out: dict[str, Any] = {
                    "metrics": metrics,
                    "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res},
                }
                if warnings:
                    out["warnings"] = warnings
                return out
            finally:
                await data_peer.close()
                await control_peer.close()
        finally:
            with contextlib.suppress(Exception):
                data_sock.close()
            with contextlib.suppress(Exception):
                control_sock.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_starway_ucx(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import starway  # type: ignore

        from bench.comparators.starway_ucx import StarwayServerPeer

        _client_role, server_role = _load_roles(workload)

        server = starway.Server()
        try:
            worker_addr = server.listen_address()
            send_conn.send({"type": "listen_address", "worker_address": worker_addr})

            deadline = asyncio.get_running_loop().time() + float(
                comparator_options.get("accept_timeout_s", 5.0)
            )
            endpoint = None
            while asyncio.get_running_loop().time() < deadline:
                clients = server.list_clients()
                if clients:
                    endpoint = next(iter(clients))
                    break
                await asyncio.sleep(0.01)
            if endpoint is None:
                raise TimeoutError(
                    "Timed out waiting for Starway server to accept client"
                )

            peer = StarwayServerPeer(server=server, endpoint=endpoint)
            try:
                if payload_verify == "preflight" and preflight_sizes:
                    from bench.preflight import preflight_verify_peer_server

                    await preflight_verify_peer_server(
                        peer, sizes=preflight_sizes, seed=int(preflight_seed)
                    )
                await server_role(peer, params)
            finally:
                await peer.close()
        finally:
            with contextlib.suppress(Exception):
                await server.aclose()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_starway_ucx(
    send_conn,
    *,
    comparator_env: dict[str, str],
    worker_address: bytes,
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import starway  # type: ignore

        from bench.comparators.starway_ucx import StarwayClientPeer

        client_role, _server_role = _load_roles(workload)

        client = starway.Client()
        await client.aconnect_address(worker_address)
        peer = StarwayClientPeer(client=client)
        try:
            if payload_verify == "preflight" and preflight_sizes:
                from bench.preflight import preflight_verify_peer_client

                await preflight_verify_peer_client(
                    peer, sizes=preflight_sizes, seed=int(preflight_seed)
                )
            return await client_role(peer, params)
        finally:
            await peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_starway_tcp(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
    bind_host: str,
    bind_port: int,
) -> None:
    """Starway UCX server via sockaddr (ip:port)."""

    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import starway  # type: ignore

        from bench.comparators.starway_tcp import StarwayServerPeer

        _client_role, server_role = _load_roles(workload)

        host = str(bind_host)
        port = int(bind_port)
        if port <= 0:
            raise ValueError("starway_tcp bind_port must be > 0")

        server = starway.Server()
        try:
            server.listen(host, port)
            send_conn.send({"type": "listen", "host": host, "port": port})

            # Wait for endpoint discovery.
            accept_timeout_s = float(comparator_options.get("accept_timeout_s", 5.0))
            deadline = asyncio.get_running_loop().time() + accept_timeout_s
            endpoint = None
            while asyncio.get_running_loop().time() < deadline:
                clients = server.list_clients()
                if clients:
                    endpoint = next(iter(clients))
                    break
                await asyncio.sleep(0.01)

            if endpoint is None:
                raise TimeoutError(
                    "Timed out waiting for Starway server to accept client"
                )

            peer = StarwayServerPeer(server=server, endpoint=endpoint)
            try:
                if payload_verify == "preflight" and preflight_sizes:
                    from bench.preflight import preflight_verify_peer_server

                    await preflight_verify_peer_server(
                        peer, sizes=preflight_sizes, seed=int(preflight_seed)
                    )
                await server_role(peer, params)
            finally:
                await peer.close()
        finally:
            with contextlib.suppress(Exception):
                await server.aclose()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_starway_tcp(
    send_conn,
    *,
    comparator_env: dict[str, str],
    host: str,
    port: int,
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    """Starway UCX client via sockaddr (ip:port)."""

    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import starway  # type: ignore

        from bench.comparators.starway_tcp import StarwayClientPeer

        client_role, _server_role = _load_roles(workload)

        client = starway.Client()
        await client.aconnect(str(host), int(port))
        peer = StarwayClientPeer(client=client)
        try:
            if payload_verify == "preflight" and preflight_sizes:
                from bench.preflight import preflight_verify_peer_client

                await preflight_verify_peer_client(
                    peer, sizes=preflight_sizes, seed=int(preflight_seed)
                )
            return await client_role(peer, params)
        finally:
            await peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_starway_tcp_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
    bind_host: str,
    data_bind_port: int,
    control_bind_port: int,
) -> None:
    """Starway UCX server (sockaddr) with two independent connections."""

    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import starway  # type: ignore

        from bench.comparators.starway_tcp import StarwayServerPeer
        from bench.workloads import bulk_stream_server, control_rtt_server

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        host = str(bind_host)
        data_port = int(data_bind_port)
        control_port = int(control_bind_port)
        if data_port <= 0 or control_port <= 0:
            raise ValueError("starway_tcp ports must be > 0")
        if data_port == control_port:
            raise ValueError("data/control ports must be distinct")

        data_server = starway.Server()
        control_server = starway.Server()
        try:
            data_server.listen(host, data_port)
            control_server.listen(host, control_port)
            send_conn.send(
                {
                    "type": "listen2",
                    "host": host,
                    "data_port": data_port,
                    "control_port": control_port,
                }
            )

            accept_timeout_s = float(comparator_options.get("accept_timeout_s", 5.0))

            async def wait_endpoint(server) -> Any:
                deadline = asyncio.get_running_loop().time() + accept_timeout_s
                while asyncio.get_running_loop().time() < deadline:
                    clients = server.list_clients()
                    if clients:
                        return next(iter(clients))
                    await asyncio.sleep(0.01)
                raise TimeoutError(
                    "Timed out waiting for Starway server to accept client"
                )

            data_endpoint = await wait_endpoint(data_server)
            control_endpoint = await wait_endpoint(control_server)

            data_peer = StarwayServerPeer(server=data_server, endpoint=data_endpoint)
            control_peer = StarwayServerPeer(
                server=control_server, endpoint=control_endpoint
            )

            async def run_control_twice() -> None:
                await control_rtt_server(control_peer, control_params)
                await control_rtt_server(control_peer, control_params)

            try:
                if payload_verify == "preflight":
                    from bench.preflight import preflight_verify_peer_server

                    if data_preflight_sizes:
                        await preflight_verify_peer_server(
                            data_peer,
                            sizes=data_preflight_sizes,
                            seed=int(preflight_seed),
                        )
                    if control_preflight_sizes:
                        await preflight_verify_peer_server(
                            control_peer,
                            sizes=control_preflight_sizes,
                            seed=int(preflight_seed),
                        )

                await asyncio.gather(
                    bulk_stream_server(data_peer, bulk_params),
                    run_control_twice(),
                )
            finally:
                await data_peer.close()
                await control_peer.close()
        finally:
            with contextlib.suppress(Exception):
                await data_server.aclose()
            with contextlib.suppress(Exception):
                await control_server.aclose()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_starway_tcp_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    host: str,
    data_port: int,
    control_port: int,
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    """Starway UCX client (sockaddr) with two independent connections."""

    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import starway  # type: ignore

        from bench.comparators.starway_tcp import StarwayClientPeer
        from bench.workloads import bulk_stream_client, control_rtt_client

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        data_client = starway.Client()
        control_client = starway.Client()
        await data_client.aconnect(str(host), int(data_port))
        await control_client.aconnect(str(host), int(control_port))
        data_peer = StarwayClientPeer(client=data_client)
        control_peer = StarwayClientPeer(client=control_client)
        try:
            if payload_verify == "preflight":
                from bench.preflight import preflight_verify_peer_client

                if data_preflight_sizes:
                    await preflight_verify_peer_client(
                        data_peer, sizes=data_preflight_sizes, seed=int(preflight_seed)
                    )
                if control_preflight_sizes:
                    await preflight_verify_peer_client(
                        control_peer,
                        sizes=control_preflight_sizes,
                        seed=int(preflight_seed),
                    )

            idle = await control_rtt_client(control_peer, control_params)
            idle_p99 = float(idle.get("metrics", {}).get("rtt_p99_us", 0.0))

            async with asyncio.TaskGroup() as tg:
                bulk_t = tg.create_task(bulk_stream_client(data_peer, bulk_params))
                loaded_t = tg.create_task(
                    control_rtt_client(control_peer, control_params)
                )

            loaded = loaded_t.result()
            bulk_res = bulk_t.result()

            loaded_p99 = float(loaded.get("metrics", {}).get("rtt_p99_us", 0.0))
            inflation = (loaded_p99 / idle_p99) if idle_p99 > 0 else 0.0

            metrics = {
                "control_idle_p99_us": idle_p99,
                "control_loaded_p99_us": loaded_p99,
                "p99_inflation_ratio": float(inflation),
                "bulk_goodput_gbps": float(
                    bulk_res.get("metrics", {}).get("goodput_gbps", 0.0)
                ),
            }

            warnings: list[str] = []
            for sub in (idle, loaded, bulk_res):
                warnings.extend(list(sub.get("warnings", [])))

            seen: set[str] = set()
            warnings = [w for w in warnings if not (w in seen or seen.add(w))]

            out: dict[str, Any] = {
                "metrics": metrics,
                "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res},
            }
            if warnings:
                out["warnings"] = warnings
            return out
        finally:
            await data_peer.close()
            await control_peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_starway_ucx_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        import starway  # type: ignore

        from bench.comparators.starway_ucx import StarwayServerPeer
        from bench.workloads import bulk_stream_server, control_rtt_server

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        data_server = starway.Server()
        control_server = starway.Server()
        try:
            data_worker_addr = data_server.listen_address()
            control_worker_addr = control_server.listen_address()
            send_conn.send(
                {
                    "type": "listen_address2",
                    "data_worker_address": data_worker_addr,
                    "control_worker_address": control_worker_addr,
                }
            )

            accept_timeout_s = float(comparator_options.get("accept_timeout_s", 5.0))

            async def wait_endpoint(server) -> Any:
                deadline = asyncio.get_running_loop().time() + accept_timeout_s
                while asyncio.get_running_loop().time() < deadline:
                    clients = server.list_clients()
                    if clients:
                        return next(iter(clients))
                    await asyncio.sleep(0.01)
                raise TimeoutError(
                    "Timed out waiting for Starway server to accept client"
                )

            data_endpoint = await wait_endpoint(data_server)
            control_endpoint = await wait_endpoint(control_server)

            data_peer = StarwayServerPeer(server=data_server, endpoint=data_endpoint)
            control_peer = StarwayServerPeer(
                server=control_server, endpoint=control_endpoint
            )

            async def run_control_twice() -> None:
                await control_rtt_server(control_peer, control_params)
                await control_rtt_server(control_peer, control_params)

            try:
                if payload_verify == "preflight":
                    from bench.preflight import preflight_verify_peer_server

                    if data_preflight_sizes:
                        await preflight_verify_peer_server(
                            data_peer,
                            sizes=data_preflight_sizes,
                            seed=int(preflight_seed),
                        )
                    if control_preflight_sizes:
                        await preflight_verify_peer_server(
                            control_peer,
                            sizes=control_preflight_sizes,
                            seed=int(preflight_seed),
                        )

                await asyncio.gather(
                    bulk_stream_server(data_peer, bulk_params),
                    run_control_twice(),
                )
            finally:
                await data_peer.close()
                await control_peer.close()
        finally:
            with contextlib.suppress(Exception):
                await data_server.aclose()
            with contextlib.suppress(Exception):
                await control_server.aclose()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_starway_ucx_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    data_worker_address: bytes,
    control_worker_address: bytes,
    comparator_options: dict[str, Any],
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        import starway  # type: ignore

        from bench.comparators.starway_ucx import StarwayClientPeer
        from bench.workloads import bulk_stream_client, control_rtt_client

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        data_client = starway.Client()
        control_client = starway.Client()
        await data_client.aconnect_address(data_worker_address)
        await control_client.aconnect_address(control_worker_address)
        data_peer = StarwayClientPeer(client=data_client)
        control_peer = StarwayClientPeer(client=control_client)
        try:
            if payload_verify == "preflight":
                from bench.preflight import preflight_verify_peer_client

                if data_preflight_sizes:
                    await preflight_verify_peer_client(
                        data_peer, sizes=data_preflight_sizes, seed=int(preflight_seed)
                    )
                if control_preflight_sizes:
                    await preflight_verify_peer_client(
                        control_peer,
                        sizes=control_preflight_sizes,
                        seed=int(preflight_seed),
                    )

            idle = await control_rtt_client(control_peer, control_params)
            idle_p99 = float(idle.get("metrics", {}).get("rtt_p99_us", 0.0))

            async with asyncio.TaskGroup() as tg:
                bulk_t = tg.create_task(bulk_stream_client(data_peer, bulk_params))
                loaded_t = tg.create_task(
                    control_rtt_client(control_peer, control_params)
                )

            loaded = loaded_t.result()
            bulk_res = bulk_t.result()

            loaded_p99 = float(loaded.get("metrics", {}).get("rtt_p99_us", 0.0))
            inflation = (loaded_p99 / idle_p99) if idle_p99 > 0 else 0.0

            metrics = {
                "control_idle_p99_us": idle_p99,
                "control_loaded_p99_us": loaded_p99,
                "p99_inflation_ratio": float(inflation),
                "bulk_goodput_gbps": float(
                    bulk_res.get("metrics", {}).get("goodput_gbps", 0.0)
                ),
            }

            warnings: list[str] = []
            for sub in (idle, loaded, bulk_res):
                warnings.extend(list(sub.get("warnings", [])))

            # De-dup while preserving order.
            seen: set[str] = set()
            warnings = [w for w in warnings if not (w in seen or seen.add(w))]

            out: dict[str, Any] = {
                "metrics": metrics,
                "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res},
            }
            if warnings:
                out["warnings"] = warnings
            return out
        finally:
            await data_peer.close()
            await control_peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_shm_py(
    send_conn,
    *,
    comparator_env: dict[str, str],
    ring_ab_spec,
    ring_ba_spec,
    spin_yield_every: int,
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        from bench.comparators.shm_py import ShmPeer
        from bench.shm_ring import attach_ring

        _client_role, server_role = _load_roles(workload)

        recv_ring = attach_ring(ring_ab_spec, spin_yield_every=int(spin_yield_every))
        send_ring = attach_ring(ring_ba_spec, spin_yield_every=int(spin_yield_every))
        peer = ShmPeer(_send_ring=send_ring, _recv_ring=recv_ring)

        send_conn.send({"type": "ready"})
        try:
            if payload_verify == "preflight" and preflight_sizes:
                from bench.preflight import preflight_verify_peer_server

                await preflight_verify_peer_server(
                    peer, sizes=preflight_sizes, seed=int(preflight_seed)
                )
            await server_role(peer, params)
        finally:
            await peer.close()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_shm_py(
    send_conn,
    *,
    comparator_env: dict[str, str],
    ring_ab_spec,
    ring_ba_spec,
    spin_yield_every: int,
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        from bench.comparators.shm_py import ShmPeer
        from bench.shm_ring import attach_ring

        client_role, _server_role = _load_roles(workload)

        send_ring = attach_ring(ring_ab_spec, spin_yield_every=int(spin_yield_every))
        recv_ring = attach_ring(ring_ba_spec, spin_yield_every=int(spin_yield_every))
        peer = ShmPeer(_send_ring=send_ring, _recv_ring=recv_ring)
        try:
            if payload_verify == "preflight" and preflight_sizes:
                from bench.preflight import preflight_verify_peer_client

                await preflight_verify_peer_client(
                    peer, sizes=preflight_sizes, seed=int(preflight_seed)
                )
            return await client_role(peer, params)
        finally:
            await peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _server_worker_shm_py_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    data_ring_ab_spec,
    data_ring_ba_spec,
    control_ring_ab_spec,
    control_ring_ba_spec,
    spin_yield_every: int,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> None:
        from bench.comparators.shm_py import ShmPeer
        from bench.shm_ring import attach_ring
        from bench.workloads import bulk_stream_server, control_rtt_server

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        data_recv_ring = attach_ring(
            data_ring_ab_spec, spin_yield_every=int(spin_yield_every)
        )
        data_send_ring = attach_ring(
            data_ring_ba_spec, spin_yield_every=int(spin_yield_every)
        )
        control_recv_ring = attach_ring(
            control_ring_ab_spec, spin_yield_every=int(spin_yield_every)
        )
        control_send_ring = attach_ring(
            control_ring_ba_spec, spin_yield_every=int(spin_yield_every)
        )

        data_peer = ShmPeer(_send_ring=data_send_ring, _recv_ring=data_recv_ring)
        control_peer = ShmPeer(
            _send_ring=control_send_ring, _recv_ring=control_recv_ring
        )

        send_conn.send({"type": "ready"})
        try:
            if payload_verify == "preflight":
                from bench.preflight import preflight_verify_peer_server

                if data_preflight_sizes:
                    await preflight_verify_peer_server(
                        data_peer, sizes=data_preflight_sizes, seed=int(preflight_seed)
                    )
                if control_preflight_sizes:
                    await preflight_verify_peer_server(
                        control_peer,
                        sizes=control_preflight_sizes,
                        seed=int(preflight_seed),
                    )

            async def run_control_twice() -> None:
                await control_rtt_server(control_peer, control_params)
                await control_rtt_server(control_peer, control_params)

            await asyncio.gather(
                bulk_stream_server(data_peer, bulk_params),
                run_control_twice(),
            )
        finally:
            await data_peer.close()
            await control_peer.close()

        send_conn.send({"type": "done"})

    try:
        asyncio.run(main())
    except BaseException as exc:
        send_conn.send(
            {
                "type": "error",
                "where": "server",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def _client_worker_shm_py_control_under_load(
    send_conn,
    *,
    comparator_env: dict[str, str],
    data_ring_ab_spec,
    data_ring_ba_spec,
    control_ring_ab_spec,
    control_ring_ba_spec,
    spin_yield_every: int,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...],
    payload_verify: PayloadVerifyMode,
    data_preflight_sizes: tuple[int, ...],
    control_preflight_sizes: tuple[int, ...],
    preflight_seed: int,
) -> None:
    _apply_env(comparator_env)
    _apply_cpu_affinity(cpu_affinity)

    async def main() -> dict[str, Any]:
        from bench.comparators.shm_py import ShmPeer
        from bench.shm_ring import attach_ring
        from bench.workloads import bulk_stream_client, control_rtt_client

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")
        control_params = dict(control_params)
        bulk_params = dict(bulk_params)

        data_send_ring = attach_ring(
            data_ring_ab_spec, spin_yield_every=int(spin_yield_every)
        )
        data_recv_ring = attach_ring(
            data_ring_ba_spec, spin_yield_every=int(spin_yield_every)
        )
        control_send_ring = attach_ring(
            control_ring_ab_spec, spin_yield_every=int(spin_yield_every)
        )
        control_recv_ring = attach_ring(
            control_ring_ba_spec, spin_yield_every=int(spin_yield_every)
        )

        data_peer = ShmPeer(_send_ring=data_send_ring, _recv_ring=data_recv_ring)
        control_peer = ShmPeer(
            _send_ring=control_send_ring, _recv_ring=control_recv_ring
        )
        try:
            if payload_verify == "preflight":
                from bench.preflight import preflight_verify_peer_client

                if data_preflight_sizes:
                    await preflight_verify_peer_client(
                        data_peer, sizes=data_preflight_sizes, seed=int(preflight_seed)
                    )
                if control_preflight_sizes:
                    await preflight_verify_peer_client(
                        control_peer,
                        sizes=control_preflight_sizes,
                        seed=int(preflight_seed),
                    )

            idle = await control_rtt_client(control_peer, control_params)
            idle_p99 = float(idle.get("metrics", {}).get("rtt_p99_us", 0.0))

            async with asyncio.TaskGroup() as tg:
                bulk_t = tg.create_task(bulk_stream_client(data_peer, bulk_params))
                loaded_t = tg.create_task(
                    control_rtt_client(control_peer, control_params)
                )

            loaded = loaded_t.result()
            bulk_res = bulk_t.result()

            loaded_p99 = float(loaded.get("metrics", {}).get("rtt_p99_us", 0.0))
            inflation = (loaded_p99 / idle_p99) if idle_p99 > 0 else 0.0

            metrics = {
                "control_idle_p99_us": idle_p99,
                "control_loaded_p99_us": loaded_p99,
                "p99_inflation_ratio": float(inflation),
                "bulk_goodput_gbps": float(
                    bulk_res.get("metrics", {}).get("goodput_gbps", 0.0)
                ),
            }

            warnings: list[str] = []
            for sub in (idle, loaded, bulk_res):
                warnings.extend(list(sub.get("warnings", [])))

            # De-dup while preserving order.
            seen: set[str] = set()
            warnings = [w for w in warnings if not (w in seen or seen.add(w))]

            out: dict[str, Any] = {
                "metrics": metrics,
                "subresults": {"idle": idle, "loaded": loaded, "bulk": bulk_res},
            }
            if warnings:
                out["warnings"] = warnings
            return out
        finally:
            await data_peer.close()
            await control_peer.close()

    try:
        result = asyncio.run(main())
        send_conn.send({"type": "result", "ok": True, "result": result})
    except BaseException as exc:
        send_conn.send(
            {
                "type": "result",
                "ok": False,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise


async def run_two_process_point(
    *,
    comparator_type: str,
    comparator_env: dict[str, str],
    comparator_options: dict[str, Any],
    workload: str,
    params: dict[str, Any],
    cpu_affinity: tuple[int, ...] = (),
    payload_verify: PayloadVerifyMode = "none",
) -> dict[str, Any]:
    """Run a single point with endpoints in two separate processes."""

    if payload_verify not in ("none", "preflight", "full"):
        raise ValueError(f"Unknown payload_verify mode '{payload_verify}'")
    if payload_verify == "full":
        raise NotImplementedError("payload_verify='full' not wired for 2proc yet")

    # control_under_load is special: it needs *two independent connections*
    # between the same two processes (data + control).
    if workload == "control_under_load":
        if comparator_type not in ("asyncio_tcp", "starway_ucx", "shm_py"):
            raise NotImplementedError(
                "2proc control_under_load currently supports asyncio_tcp, starway_ucx, and shm_py"
            )

        control_params = params.get("control", {})
        bulk_params = params.get("bulk", {})
        if not isinstance(control_params, dict):
            raise ValueError("control_under_load.params.control must be a mapping")
        if not isinstance(bulk_params, dict):
            raise ValueError("control_under_load.params.bulk must be a mapping")

        preflight_seed = int(params.get("seed", 0))
        data_preflight_sizes: tuple[int, ...] = ()
        control_preflight_sizes: tuple[int, ...] = ()
        if payload_verify == "preflight":
            data_preflight_sizes = _preflight_sizes_for_workload(
                "bulk_stream", dict(bulk_params)
            )
            control_preflight_sizes = _preflight_sizes_for_workload(
                "control_rtt", dict(control_params)
            )

        mp = multiprocessing.get_context("spawn")
        server_recv, server_send = mp.Pipe(duplex=False)
        client_recv, client_send = mp.Pipe(duplex=False)

        # If provided, use first CPU for server and second for client.
        server_aff = (cpu_affinity[0],) if len(cpu_affinity) >= 1 else ()
        client_aff = (cpu_affinity[1],) if len(cpu_affinity) >= 2 else server_aff

        ring_cleanup = contextlib.ExitStack()
        server_proc = None
        client_proc = None
        try:
            data_ring_ab_spec = None
            data_ring_ba_spec = None
            control_ring_ab_spec = None
            control_ring_ba_spec = None
            data_slots: int | None = None
            control_slots: int | None = None
            data_slot_payload_bytes: int | None = None
            control_slot_payload_bytes: int | None = None
            shm_target_bytes: int | None = None
            shm_spin_yield_every: int | None = None

            if comparator_type == "shm_py":
                from bench.shm_ring import create_ring

                data_slot_payload_bytes = _max_payload_bytes_for_workload(
                    "bulk_stream", dict(bulk_params)
                )
                control_slot_payload_bytes = _max_payload_bytes_for_workload(
                    "control_rtt", dict(control_params)
                )
                shm_target_bytes = _parse_bytes(
                    comparator_options.get("shm_target_bytes", 64 * 1024 * 1024)
                )
                shm_min_slots = int(comparator_options.get("shm_min_slots", 4))
                shm_max_slots = int(comparator_options.get("shm_max_slots", 65536))
                shm_spin_yield_every = int(
                    comparator_options.get("spin_yield_every", 1024)
                )

                if shm_target_bytes <= 0:
                    raise ValueError(
                        "shm_py comparator_options.shm_target_bytes must be > 0"
                    )
                if shm_min_slots <= 0:
                    raise ValueError(
                        "shm_py comparator_options.shm_min_slots must be > 0"
                    )
                if shm_max_slots <= 0:
                    raise ValueError(
                        "shm_py comparator_options.shm_max_slots must be > 0"
                    )
                if shm_spin_yield_every <= 0:
                    raise ValueError(
                        "shm_py comparator_options.spin_yield_every must be > 0"
                    )

                data_slots = _choose_shm_slots(
                    slot_payload_bytes=data_slot_payload_bytes,
                    target_bytes=shm_target_bytes,
                    min_slots=shm_min_slots,
                    max_slots=shm_max_slots,
                )
                control_slots = _choose_shm_slots(
                    slot_payload_bytes=control_slot_payload_bytes,
                    target_bytes=shm_target_bytes,
                    min_slots=shm_min_slots,
                    max_slots=shm_max_slots,
                )
                if data_slots <= 0 or control_slots <= 0:
                    raise ValueError("shm_py computed ring slots must be > 0")

                def _safe_close_owner(owner) -> None:
                    with contextlib.suppress(Exception):
                        owner.close()

                data_ring_ab_spec, data_ring_ab_owner = create_ring(
                    slots=data_slots,
                    slot_payload_bytes=data_slot_payload_bytes,
                    spin_yield_every=shm_spin_yield_every,
                )
                ring_cleanup.callback(_safe_close_owner, data_ring_ab_owner)
                data_ring_ba_spec, data_ring_ba_owner = create_ring(
                    slots=data_slots,
                    slot_payload_bytes=data_slot_payload_bytes,
                    spin_yield_every=shm_spin_yield_every,
                )
                ring_cleanup.callback(_safe_close_owner, data_ring_ba_owner)
                control_ring_ab_spec, control_ring_ab_owner = create_ring(
                    slots=control_slots,
                    slot_payload_bytes=control_slot_payload_bytes,
                    spin_yield_every=shm_spin_yield_every,
                )
                ring_cleanup.callback(_safe_close_owner, control_ring_ab_owner)
                control_ring_ba_spec, control_ring_ba_owner = create_ring(
                    slots=control_slots,
                    slot_payload_bytes=control_slot_payload_bytes,
                    spin_yield_every=shm_spin_yield_every,
                )
                ring_cleanup.callback(_safe_close_owner, control_ring_ba_owner)

            # Start server.
            if comparator_type == "asyncio_tcp":
                server_target = _server_worker_asyncio_tcp_control_under_load
                server_kwargs = {
                    "send_conn": server_send,
                    "comparator_env": dict(comparator_env),
                    "comparator_options": dict(comparator_options),
                    "params": dict(params),
                    "cpu_affinity": tuple(server_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }
            elif comparator_type == "starway_ucx":
                server_target = _server_worker_starway_ucx_control_under_load
                server_kwargs = {
                    "send_conn": server_send,
                    "comparator_env": dict(comparator_env),
                    "comparator_options": dict(comparator_options),
                    "params": dict(params),
                    "cpu_affinity": tuple(server_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }
            else:
                assert data_ring_ab_spec is not None
                assert data_ring_ba_spec is not None
                assert control_ring_ab_spec is not None
                assert control_ring_ba_spec is not None
                assert shm_spin_yield_every is not None
                server_target = _server_worker_shm_py_control_under_load
                server_kwargs = {
                    "send_conn": server_send,
                    "comparator_env": dict(comparator_env),
                    "data_ring_ab_spec": data_ring_ab_spec,
                    "data_ring_ba_spec": data_ring_ba_spec,
                    "control_ring_ab_spec": control_ring_ab_spec,
                    "control_ring_ba_spec": control_ring_ba_spec,
                    "spin_yield_every": shm_spin_yield_every,
                    "params": dict(params),
                    "cpu_affinity": tuple(server_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }

            server_proc = mp.Process(
                target=server_target, kwargs=server_kwargs, daemon=True
            )
            server_proc.start()

            msg = await _recv_with_timeout(server_recv, timeout_s=5.0)
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("error"))

            if comparator_type == "asyncio_tcp":
                if msg.get("type") != "listen2":
                    raise RuntimeError(f"unexpected server message: {msg}")
                host = str(msg["host"])
                data_port = int(msg["data_port"])
                control_port = int(msg["control_port"])
                client_target = _client_worker_asyncio_tcp_control_under_load
                client_kwargs = {
                    "send_conn": client_send,
                    "comparator_env": dict(comparator_env),
                    "host": host,
                    "data_port": data_port,
                    "control_port": control_port,
                    "comparator_options": dict(comparator_options),
                    "params": dict(params),
                    "cpu_affinity": tuple(client_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }
            elif comparator_type == "starway_ucx":
                if msg.get("type") != "listen_address2":
                    raise RuntimeError(f"unexpected server message: {msg}")
                data_worker_address = msg["data_worker_address"]
                control_worker_address = msg["control_worker_address"]
                client_target = _client_worker_starway_ucx_control_under_load
                client_kwargs = {
                    "send_conn": client_send,
                    "comparator_env": dict(comparator_env),
                    "data_worker_address": data_worker_address,
                    "control_worker_address": control_worker_address,
                    "comparator_options": dict(comparator_options),
                    "params": dict(params),
                    "cpu_affinity": tuple(client_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }
            else:
                if msg.get("type") != "ready":
                    raise RuntimeError(f"unexpected server message: {msg}")
                assert data_ring_ab_spec is not None
                assert data_ring_ba_spec is not None
                assert control_ring_ab_spec is not None
                assert control_ring_ba_spec is not None
                assert shm_spin_yield_every is not None
                client_target = _client_worker_shm_py_control_under_load
                client_kwargs = {
                    "send_conn": client_send,
                    "comparator_env": dict(comparator_env),
                    "data_ring_ab_spec": data_ring_ab_spec,
                    "data_ring_ba_spec": data_ring_ba_spec,
                    "control_ring_ab_spec": control_ring_ab_spec,
                    "control_ring_ba_spec": control_ring_ba_spec,
                    "spin_yield_every": shm_spin_yield_every,
                    "params": dict(params),
                    "cpu_affinity": tuple(client_aff),
                    "payload_verify": payload_verify,
                    "data_preflight_sizes": data_preflight_sizes,
                    "control_preflight_sizes": control_preflight_sizes,
                    "preflight_seed": preflight_seed,
                }

            client_proc = mp.Process(
                target=client_target, kwargs=client_kwargs, daemon=True
            )
            client_proc.start()

            client_msg = await _recv_with_timeout(client_recv, timeout_s=20.0)
            if client_msg.get("type") != "result":
                raise RuntimeError(f"unexpected client message: {client_msg}")
            if not client_msg.get("ok", False):
                raise RuntimeError(client_msg.get("error"))

            server_done = await _recv_with_timeout(server_recv, timeout_s=20.0)
            if server_done.get("type") == "error":
                raise RuntimeError(server_done.get("error"))
            if server_done.get("type") != "done":
                raise RuntimeError(f"unexpected server message: {server_done}")

            result = client_msg["result"]
            if comparator_type == "shm_py":
                metrics = result.setdefault("metrics", {})
                if data_slots is not None:
                    metrics.setdefault("data_shm_slots", float(data_slots))
                if control_slots is not None:
                    metrics.setdefault("control_shm_slots", float(control_slots))
                if data_slot_payload_bytes is not None:
                    metrics.setdefault(
                        "data_shm_slot_payload_bytes", float(data_slot_payload_bytes)
                    )
                if control_slot_payload_bytes is not None:
                    metrics.setdefault(
                        "control_shm_slot_payload_bytes",
                        float(control_slot_payload_bytes),
                    )
                if shm_target_bytes is not None:
                    metrics.setdefault("shm_target_bytes", float(shm_target_bytes))

                warnings = list(result.get("warnings", []))
                inflight = int(dict(bulk_params).get("inflight", 0))
                if inflight > 0 and data_slots is not None and data_slots < inflight:
                    warnings.append(
                        f"shm_py: ring slots ({data_slots}) < bulk inflight ({inflight}); "
                        "effective inflight is clamped by shm backpressure"
                    )
                if warnings:
                    seen: set[str] = set()
                    result["warnings"] = [
                        w for w in warnings if not (w in seen or seen.add(w))
                    ]

            return result
        finally:
            for proc in (client_proc, server_proc):
                if proc is None:
                    continue
                if proc.is_alive():
                    proc.terminate()
                proc.join(timeout=2.0)
            ring_cleanup.close()

    preflight_seed = int(params.get("seed", 0))
    preflight_sizes: tuple[int, ...] = ()
    if payload_verify == "preflight":
        preflight_sizes = _preflight_sizes_for_workload(workload, params)

    if comparator_type not in ("asyncio_tcp", "starway_ucx", "shm_py"):
        raise NotImplementedError(
            "2proc currently supports only asyncio_tcp, starway_ucx, and shm_py"
        )

    mp = multiprocessing.get_context("spawn")

    shm_ring_ab_owner = None
    shm_ring_ba_owner = None
    shm_ring_ab_spec = None
    shm_ring_ba_spec = None
    shm_slots: int | None = None
    shm_slot_payload_bytes: int | None = None
    shm_target_bytes: int | None = None
    shm_spin_yield_every: int | None = None

    if comparator_type == "shm_py":
        from bench.shm_ring import create_ring

        shm_slot_payload_bytes = _max_payload_bytes_for_workload(workload, params)
        shm_target_bytes = _parse_bytes(
            comparator_options.get("shm_target_bytes", 64 * 1024 * 1024)
        )
        shm_min_slots = int(comparator_options.get("shm_min_slots", 4))
        shm_max_slots = int(comparator_options.get("shm_max_slots", 65536))
        shm_spin_yield_every = int(comparator_options.get("spin_yield_every", 1024))

        if shm_target_bytes <= 0:
            raise ValueError("shm_py comparator_options.shm_target_bytes must be > 0")
        if shm_min_slots <= 0:
            raise ValueError("shm_py comparator_options.shm_min_slots must be > 0")
        if shm_max_slots <= 0:
            raise ValueError("shm_py comparator_options.shm_max_slots must be > 0")
        if shm_spin_yield_every <= 0:
            raise ValueError("shm_py comparator_options.spin_yield_every must be > 0")

        shm_slots = _choose_shm_slots(
            slot_payload_bytes=shm_slot_payload_bytes,
            target_bytes=shm_target_bytes,
            min_slots=shm_min_slots,
            max_slots=shm_max_slots,
        )
        if shm_slots <= 0:
            raise ValueError("shm_py computed ring slots must be > 0")

        shm_ring_ab_spec, shm_ring_ab_owner = create_ring(
            slots=shm_slots,
            slot_payload_bytes=shm_slot_payload_bytes,
            spin_yield_every=shm_spin_yield_every,
        )
        shm_ring_ba_spec, shm_ring_ba_owner = create_ring(
            slots=shm_slots,
            slot_payload_bytes=shm_slot_payload_bytes,
            spin_yield_every=shm_spin_yield_every,
        )

    server_recv, server_send = mp.Pipe(duplex=False)
    client_recv, client_send = mp.Pipe(duplex=False)

    # If provided, use first CPU for server and second for client.
    server_aff = (cpu_affinity[0],) if len(cpu_affinity) >= 1 else ()
    client_aff = (cpu_affinity[1],) if len(cpu_affinity) >= 2 else server_aff

    if comparator_type == "asyncio_tcp":
        server_target = _server_worker_asyncio_tcp
        server_kwargs = {
            "send_conn": server_send,
            "comparator_env": dict(comparator_env),
            "comparator_options": dict(comparator_options),
            "workload": str(workload),
            "params": dict(params),
            "cpu_affinity": tuple(server_aff),
            "payload_verify": payload_verify,
            "preflight_sizes": preflight_sizes,
            "preflight_seed": preflight_seed,
        }
    elif comparator_type == "starway_ucx":
        server_target = _server_worker_starway_ucx
        server_kwargs = {
            "send_conn": server_send,
            "comparator_env": dict(comparator_env),
            "comparator_options": dict(comparator_options),
            "workload": str(workload),
            "params": dict(params),
            "cpu_affinity": tuple(server_aff),
            "payload_verify": payload_verify,
            "preflight_sizes": preflight_sizes,
            "preflight_seed": preflight_seed,
        }
    else:
        assert shm_ring_ab_spec is not None
        assert shm_ring_ba_spec is not None
        assert shm_spin_yield_every is not None
        server_target = _server_worker_shm_py
        server_kwargs = {
            "send_conn": server_send,
            "comparator_env": dict(comparator_env),
            "ring_ab_spec": shm_ring_ab_spec,
            "ring_ba_spec": shm_ring_ba_spec,
            "spin_yield_every": shm_spin_yield_every,
            "workload": str(workload),
            "params": dict(params),
            "cpu_affinity": tuple(server_aff),
            "payload_verify": payload_verify,
            "preflight_sizes": preflight_sizes,
            "preflight_seed": preflight_seed,
        }

    server_proc = mp.Process(
        target=server_target,
        kwargs=server_kwargs,
        daemon=True,
    )
    server_proc.start()

    client_proc = None
    try:
        msg = await _recv_with_timeout(server_recv, timeout_s=5.0)
        if msg.get("type") == "error":
            raise RuntimeError(msg.get("error"))
        if comparator_type == "asyncio_tcp":
            if msg.get("type") != "listen":
                raise RuntimeError(f"unexpected server message: {msg}")

            host = msg["host"]
            port = int(msg["port"])
            client_target = _client_worker_asyncio_tcp
            client_kwargs = {
                "send_conn": client_send,
                "comparator_env": dict(comparator_env),
                "host": host,
                "port": port,
                "comparator_options": dict(comparator_options),
                "workload": str(workload),
                "params": dict(params),
                "cpu_affinity": tuple(client_aff),
                "payload_verify": payload_verify,
                "preflight_sizes": preflight_sizes,
                "preflight_seed": preflight_seed,
            }
        elif comparator_type == "starway_ucx":
            if msg.get("type") != "listen_address":
                raise RuntimeError(f"unexpected server message: {msg}")
            worker_address = msg["worker_address"]
            client_target = _client_worker_starway_ucx
            client_kwargs = {
                "send_conn": client_send,
                "comparator_env": dict(comparator_env),
                "worker_address": worker_address,
                "comparator_options": dict(comparator_options),
                "workload": str(workload),
                "params": dict(params),
                "cpu_affinity": tuple(client_aff),
                "payload_verify": payload_verify,
                "preflight_sizes": preflight_sizes,
                "preflight_seed": preflight_seed,
            }
        else:
            if msg.get("type") != "ready":
                raise RuntimeError(f"unexpected server message: {msg}")
            assert shm_ring_ab_spec is not None
            assert shm_ring_ba_spec is not None
            assert shm_spin_yield_every is not None
            client_target = _client_worker_shm_py
            client_kwargs = {
                "send_conn": client_send,
                "comparator_env": dict(comparator_env),
                "ring_ab_spec": shm_ring_ab_spec,
                "ring_ba_spec": shm_ring_ba_spec,
                "spin_yield_every": shm_spin_yield_every,
                "workload": str(workload),
                "params": dict(params),
                "cpu_affinity": tuple(client_aff),
                "payload_verify": payload_verify,
                "preflight_sizes": preflight_sizes,
                "preflight_seed": preflight_seed,
            }

        client_proc = mp.Process(
            target=client_target,
            kwargs=client_kwargs,
            daemon=True,
        )
        client_proc.start()

        # Wait for client result and server completion.
        client_msg = await _recv_with_timeout(client_recv, timeout_s=20.0)
        if client_msg.get("type") != "result":
            raise RuntimeError(f"unexpected client message: {client_msg}")
        if not client_msg.get("ok", False):
            raise RuntimeError(client_msg.get("error"))

        server_done = await _recv_with_timeout(server_recv, timeout_s=20.0)
        if server_done.get("type") == "error":
            raise RuntimeError(server_done.get("error"))
        if server_done.get("type") != "done":
            raise RuntimeError(f"unexpected server message: {server_done}")

        result = client_msg["result"]
        if comparator_type == "shm_py":
            metrics = result.setdefault("metrics", {})
            if shm_slots is not None:
                metrics.setdefault("shm_slots", float(shm_slots))
            if shm_slot_payload_bytes is not None:
                metrics.setdefault(
                    "shm_slot_payload_bytes", float(shm_slot_payload_bytes)
                )
            if shm_target_bytes is not None:
                metrics.setdefault("shm_target_bytes", float(shm_target_bytes))

            warnings = list(result.get("warnings", []))
            if workload in ("bulk_stream", "small_message_pressure", "mixed_size"):
                inflight = int(params.get("inflight", 0))
                if inflight > 0 and shm_slots is not None and shm_slots < inflight:
                    warnings.append(
                        f"shm_py: ring slots ({shm_slots}) < workload inflight ({inflight}); "
                        "effective inflight is clamped by shm backpressure"
                    )
            if warnings:
                # De-dup while preserving order.
                seen: set[str] = set()
                result["warnings"] = [
                    w for w in warnings if not (w in seen or seen.add(w))
                ]

        return result
    finally:
        # Ensure processes are not left behind.
        for proc in (client_proc, server_proc):
            if proc is None:
                continue
            if proc.is_alive():
                proc.terminate()
            proc.join(timeout=2.0)

        if shm_ring_ab_owner is not None:
            with contextlib.suppress(Exception):
                shm_ring_ab_owner.close()
        if shm_ring_ba_owner is not None:
            with contextlib.suppress(Exception):
                shm_ring_ba_owner.close()

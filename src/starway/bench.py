from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from starway import Client, Server
from starway._bindings import ServerEndpoint  # type: ignore

# Delay importing starway bindings until after UCX env vars are configured.
from .benchmarks import get_scenario, list_scenarios
from .benchmarks.scenarios import (
    CONTROL_TAG,
    DONE_TAG,
    READY_TAG,
    SCENARIOS,
    TAG_MASK,
    ScenarioResult,
)


def parse_size(value: str) -> int:
    text = value.strip().lower().replace("_", "")
    suffixes = {
        "k": 1024,
        "kb": 1024,
        "ki": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mi": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gi": 1024**3,
        "gib": 1024**3,
    }
    for suffix, multiplier in suffixes.items():
        if text.endswith(suffix):
            number = float(text[: -len(suffix)])
            return int(number * multiplier)
    return int(float(text))


def parse_worker_address(value: str) -> bytes:
    cleaned = value.replace(":", "").replace(" ", "").strip()
    return bytes.fromhex(cleaned)


def scenario_plan(args: argparse.Namespace) -> list[tuple[str, dict[str, Any]]]:
    requested: Sequence[str]
    if not args.scenarios:
        requested = list_scenarios()
    elif len(args.scenarios) == 1 and args.scenarios[0].lower() == "all":
        requested = list_scenarios()
    else:
        requested = args.scenarios

    plan: list[tuple[str, dict[str, Any]]] = []
    for name in requested:
        if name not in SCENARIOS:
            raise ValueError(
                f"Unknown scenario '{name}'. Available: {', '.join(list_scenarios())}"
            )
        overrides: dict[str, Any] = {}
        if name == "large-array":
            if args.large_bytes is not None:
                overrides["message_bytes"] = args.large_bytes
            if args.large_iterations is not None:
                overrides["iterations"] = args.large_iterations
            if args.large_warmup is not None:
                overrides["warmup"] = args.large_warmup
        elif name == "small-messages":
            if args.small_bytes is not None:
                overrides["message_bytes"] = args.small_bytes
            if args.small_iterations is not None:
                overrides["iterations"] = args.small_iterations
            if args.small_warmup is not None:
                overrides["warmup_batches"] = args.small_warmup
            if args.small_concurrency is not None:
                overrides["concurrency"] = args.small_concurrency
        elif name == "pingpong-flag":
            if args.flag_iterations is not None:
                overrides["iterations"] = args.flag_iterations
            if args.flag_warmup is not None:
                overrides["warmup"] = args.flag_warmup
        elif name == "streaming-duplex":
            if args.stream_bytes is not None:
                overrides["message_bytes"] = args.stream_bytes
            if args.stream_iterations is not None:
                overrides["iterations"] = args.stream_iterations
            if args.stream_warmup is not None:
                overrides["warmup"] = args.stream_warmup
        plan.append((name, overrides))
    return plan


def format_metrics(metrics: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    for key, value in metrics.items():
        if isinstance(value, float):
            lines.append(f"{key}: {value:.6f}")
        else:
            lines.append(f"{key}: {value}")
    return lines


def encode_control(payload: Mapping[str, Any]) -> np.ndarray:
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return np.frombuffer(data, dtype=np.uint8).copy()


def decode_control(buffer: np.ndarray, length: int) -> Mapping[str, Any]:
    raw = memoryview(buffer)[:length].tobytes()
    return json.loads(raw.decode("utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Starway UCX benchmark suite")
    parser.add_argument(
        "--role", choices=("server", "client", "loopback"), required=True
    )
    parser.add_argument(
        "--addr", default="0.0.0.0", help="Server listen address (socket mode)."
    )
    parser.add_argument(
        "--port", type=int, default=17777, help="TCP port for socket mode."
    )
    parser.add_argument(
        "--server-host", default="127.0.0.1", help="Server hostname (client)."
    )
    parser.add_argument("--listen-mode", choices=("socket", "worker"), default="socket")
    parser.add_argument(
        "--connect-mode", choices=("socket", "worker"), default="socket"
    )
    parser.add_argument(
        "--worker-address",
        help="Hex-encoded UCX worker address for connect-mode=worker.",
    )
    parser.add_argument(
        "--tls",
        help="Comma-separated UCX_TLS string (e.g. tcp, sm, rc). Overrides environment.",
    )
    parser.add_argument(
        "--scenarios",
        nargs="*",
        help="Scenarios to execute (default: all). Options: "
        + ", ".join(list_scenarios()),
    )
    parser.add_argument(
        "--large-bytes",
        type=parse_size,
        help="Payload size for large-array (e.g. 512M).",
    )
    parser.add_argument(
        "--large-iterations", type=int, help="Measured iterations for large-array."
    )
    parser.add_argument(
        "--large-warmup", type=int, help="Warmup iterations for large-array."
    )
    parser.add_argument(
        "--small-bytes", type=parse_size, help="Message size for small-messages."
    )
    parser.add_argument(
        "--small-iterations", type=int, help="Measured batches for small-messages."
    )
    parser.add_argument(
        "--small-warmup", type=int, help="Warmup batches for small-messages."
    )
    parser.add_argument(
        "--small-concurrency", type=int, help="Concurrency for small-messages."
    )
    parser.add_argument(
        "--flag-iterations", type=int, help="Measured ping-pong iterations."
    )
    parser.add_argument("--flag-warmup", type=int, help="Warmup ping-pong iterations.")
    parser.add_argument(
        "--stream-bytes", type=parse_size, help="Chunk size for streaming-duplex."
    )
    parser.add_argument(
        "--stream-iterations", type=int, help="Measured streaming iterations."
    )
    parser.add_argument(
        "--stream-warmup", type=int, help="Warmup streaming iterations."
    )
    parser.add_argument(
        "--output", type=Path, help="Optional path to write JSON results."
    )
    parser.add_argument(
        "--store-trace",
        action="store_true",
        help="Include per-iteration samples in JSON output.",
    )
    return parser


class ClientSession:
    def __init__(self, client: Client):
        self.client = client
        self.tag_mask = TAG_MASK
        self._ready_recv = np.zeros(1, dtype=np.uint8)
        self._done_recv = np.zeros(1, dtype=np.uint8)

    async def send_control(self, payload: Mapping[str, Any]) -> None:
        buffer = encode_control(payload)
        await self.client.asend(buffer, CONTROL_TAG)
        await self.flush()

    async def wait_ready(self) -> None:
        await self.client.arecv(self._ready_recv, READY_TAG, self.tag_mask)

    async def wait_done(self) -> None:
        await self.client.arecv(self._done_recv, DONE_TAG, self.tag_mask)

    async def flush(self) -> None:
        await self.client.aflush()


class ClientScenarioContext:
    def __init__(self, session: ClientSession):
        self._session = session
        self.client = session.client
        self.tag_mask = session.tag_mask

    async def flush(self) -> None:
        await self._session.flush()


class ServerSession:
    def __init__(self, server: Server, endpoint: ServerEndpoint):
        self.server = server
        self.endpoint = endpoint
        self.tag_mask = TAG_MASK
        self._ready_buf = np.array([1], dtype=np.uint8)
        self._done_buf = np.array([1], dtype=np.uint8)

    async def recv_control(self, max_bytes: int = 4096) -> Mapping[str, Any]:
        buffer = np.empty(max_bytes, dtype=np.uint8)
        _, length = await self.server.arecv(buffer, CONTROL_TAG, self.tag_mask)
        return decode_control(buffer, length)

    async def send_ready(self) -> None:
        await self.server.asend(self.endpoint, self._ready_buf, READY_TAG)

    async def send_done(self) -> None:
        await self.server.asend(self.endpoint, self._done_buf, DONE_TAG)


class ServerScenarioContext:
    def __init__(self, session: ServerSession):
        self._session = session
        self.server = session.server
        self.endpoint = session.endpoint
        self.tag_mask = session.tag_mask

    async def signal_ready(self) -> None:
        await self._session.send_ready()

    async def flush_endpoint(self) -> None:
        await self.server.aflush_ep(self.endpoint)


async def run_client(args: argparse.Namespace) -> list[ScenarioResult]:
    # Local import to ensure UCX env state is final
    from . import Client

    client = Client()
    results: list[ScenarioResult] = []
    try:
        if args.connect_mode == "worker":
            if not args.worker_address:
                raise ValueError("--worker-address required for connect-mode=worker")
            worker_addr = parse_worker_address(args.worker_address)
            await client.aconnect_address(worker_addr)
            print(f"[client] Connected via worker address ({len(worker_addr)} bytes).")
        else:
            await client.aconnect(args.server_host, args.port)
            print(f"[client] Connected to {args.server_host}:{args.port}.")

        session = ClientSession(client)
        context = ClientScenarioContext(session)

        for scenario_name, overrides in scenario_plan(args):
            scenario = get_scenario(scenario_name)
            payload = {
                "scenario": scenario_name,
                "config": overrides,
            }
            print(
                f"[client] Starting scenario '{scenario_name}' with overrides {overrides or 'defaults'}."
            )
            await session.send_control(payload)
            await session.wait_ready()
            result = await scenario.client_runner(context, overrides)
            results.append(result)
            await session.wait_done()
            print(f"[client] Completed '{scenario_name}'.")

        await session.send_control({"scenario": "__shutdown__"})
        await session.flush()
    finally:
        await client.aclose()
    return results


async def run_server(args: argparse.Namespace) -> None:
    from . import Server

    server = Server()
    loop = asyncio.get_running_loop()
    accepted: asyncio.Queue[Any] = asyncio.Queue()

    def accept_cb(endpoint: "ServerEndpoint") -> None:
        loop.call_soon_threadsafe(accepted.put_nowait, endpoint)

    server.set_accept_cb(accept_cb)

    if args.listen_mode == "worker":
        worker_address = server.listen_address()
        print(f"[server] Listening via worker address: {worker_address.hex()}")
    else:
        server.listen(args.addr, args.port)
        print(f"[server] Listening on {args.addr}:{args.port}")

    endpoint = await accepted.get()
    print("[server] Client accepted.")
    session = ServerSession(server, endpoint)

    try:
        while True:
            control = await session.recv_control()
            scenario_name = control.get("scenario")
            if scenario_name == "__shutdown__":
                print("[server] Shutdown request received.")
                break
            if scenario_name not in SCENARIOS:
                raise ValueError(f"Unknown scenario '{scenario_name}' from client.")
            overrides = control.get("config", {})
            scenario = get_scenario(scenario_name)
            context = ServerScenarioContext(session)
            print(
                f"[server] Running scenario '{scenario_name}' with overrides {overrides or 'defaults'}."
            )
            await scenario.server_runner(context, overrides)
            await session.send_done()
            print(f"[server] Scenario '{scenario_name}' completed.")
    finally:
        await server.aclose()
        print("[server] Closed.")


async def run_loopback(args: argparse.Namespace) -> list[ScenarioResult]:
    client_done: asyncio.Future[list[ScenarioResult]] = (
        asyncio.get_running_loop().create_future()
    )

    async def client_task() -> None:
        try:
            results = await run_client(args)
            client_done.set_result(results)
        except Exception as exc:
            if not client_done.done():
                client_done.set_exception(exc)
            raise

    server_task = asyncio.create_task(run_server(args))
    client_task_future = asyncio.create_task(client_task())
    try:
        results = await client_done
    finally:
        await client_task_future
        await server_task
    return results


def dump_results(results: Sequence[ScenarioResult], args: argparse.Namespace) -> None:
    if not results:
        print("No results collected.")
        return

    print("\n=== Benchmark Results ===")
    for result in results:
        scenario = get_scenario(result.name)
        print(f"\n[{result.name}] {scenario.description}")
        for line in format_metrics(result.metrics):
            print(f"  {line}")

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "timestamp": time.time(),
            "transport": os.environ.get("UCX_TLS"),
            "scenarios": [
                res.to_dict(include_samples=args.store_trace) for res in results
            ],
        }
        args.output.write_text(json.dumps(report, indent=2))
        print(f"\nJSON results written to {args.output}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.tls:
        os.environ["UCX_TLS"] = args.tls

    role = args.role

    if role == "server":

        async def server_main() -> None:
            await run_server(args)

        asyncio.run(server_main())
        return 0

    if role == "client":
        results = asyncio.run(run_client(args))
        dump_results(results, args)
        return 0

    if role == "loopback":
        results = asyncio.run(run_loopback(args))
        dump_results(results, args)
        return 0

    raise ValueError(f"Unknown role {role}")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())

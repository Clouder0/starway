import asyncio
import asyncio
import json
import socket
import textwrap
from pathlib import Path

import pytest


def _pick_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _write_config(path: Path, toml_text: str) -> Path:
    path.write_text(textwrap.dedent(toml_text).lstrip(), encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_cluster_campaign_smoke_asyncio_tcp(tmp_path: Path) -> None:
    from bench.cluster.agent import run_agent
    from bench.run import run_campaign

    results_dir = tmp_path / "results"
    coord_port = _pick_free_port()
    port_min = 31000
    port_max = 31100

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        f"""
        [campaign]
        id = "cluster_smoke"
        results_dir = "{results_dir}"
        repeats = 1
        seed = 1

        [execution]
        mode = "cluster"
        topology = "two_process"
        point_timeout_s = 10.0
        close_timeout_s = 2.0

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = {coord_port}
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = {port_min}
        port_range_max = {port_max}
        agent_connect_timeout_s = 5.0
        agent_rpc_timeout_s = 5.0

        [[comparators]]
        name = "asyncio_tcp"
        type = "asyncio_tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [5]
        """,
    )

    campaign_task = asyncio.create_task(run_campaign(cfg_path, dry_run=False))
    try:
        # Give the coordinator a moment to start listening.
        await asyncio.sleep(0.05)

        a_task = asyncio.create_task(
            run_agent(
                agent_id="hostA",
                coordinator_host="127.0.0.1",
                coordinator_port=coord_port,
                advertise_host="127.0.0.1",
            )
        )
        b_task = asyncio.create_task(
            run_agent(
                agent_id="hostB",
                coordinator_host="127.0.0.1",
                coordinator_port=coord_port,
                advertise_host="127.0.0.1",
            )
        )

        rc = await campaign_task
        assert rc == 0

        # Agents should exit once the coordinator closes their connections.
        await asyncio.wait_for(asyncio.gather(a_task, b_task), timeout=5.0)
    finally:
        if not campaign_task.done():
            campaign_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await campaign_task

    # Find the single produced run directory.
    run_root = results_dir / "cluster_smoke"
    runs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(runs) == 1
    run_dir = runs[0]
    points_dir = run_dir / "points"
    point_files = list(points_dir.glob("*.json"))
    assert len(point_files) == 1
    rec = json.loads(point_files[0].read_text(encoding="utf-8"))
    assert rec["status"] == "ok"
    assert rec.get("connect", {}).get("host") == "127.0.0.1"
    port = int(rec.get("connect", {}).get("port", 0))
    assert port_min <= port <= port_max

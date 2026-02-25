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


def _write_text(path: Path, text: str) -> Path:
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_cluster_smoke_starway_tcp_control_rtt(tmp_path: Path) -> None:
    try:
        import starway  # noqa: F401
    except Exception:
        pytest.skip("starway bindings not available")

    from bench.cluster.agent import run_agent
    from bench.run import run_campaign

    results_dir = tmp_path / "results"
    coord_port = _pick_free_port()

    cfg_path = _write_text(
        tmp_path / "cluster.toml",
        f"""
        [campaign]
        id = "cluster_smoke_ucx"
        results_dir = "{results_dir}"
        repeats = 1
        seed = 1

        [execution]
        mode = "cluster"
        topology = "two_process"
        point_timeout_s = 20.0

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = {coord_port}
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 33000
        port_range_max = 33100
        agent_connect_timeout_s = 5.0
        agent_rpc_timeout_s = 5.0

        [[comparators]]
        name = "ucx_tcp"
        type = "starway_tcp"

        [comparators.env]
        UCX_TLS = "tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [50]
        """,
    )

    campaign_task = asyncio.create_task(run_campaign(cfg_path, dry_run=False))
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
    await asyncio.wait_for(asyncio.gather(a_task, b_task), timeout=10.0)

    run_root = results_dir / "cluster_smoke_ucx"
    run_dirs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    point_files = list((run_dirs[0] / "points").glob("*.json"))
    assert len(point_files) == 1
    rec = json.loads(point_files[0].read_text(encoding="utf-8"))
    assert rec["status"] == "ok"


@pytest.mark.asyncio
async def test_cluster_smoke_starway_tcp_control_under_load(tmp_path: Path) -> None:
    try:
        import starway  # noqa: F401
    except Exception:
        pytest.skip("starway bindings not available")

    from bench.cluster.agent import run_agent
    from bench.run import run_campaign

    results_dir = tmp_path / "results"
    coord_port = _pick_free_port()

    cfg_path = _write_text(
        tmp_path / "cluster.toml",
        f"""
        [campaign]
        id = "cluster_smoke_ucx"
        results_dir = "{results_dir}"
        repeats = 1
        seed = 1

        [execution]
        mode = "cluster"
        topology = "two_process"
        point_timeout_s = 30.0

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = {coord_port}
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 33200
        port_range_max = 33300
        agent_connect_timeout_s = 5.0
        agent_rpc_timeout_s = 5.0

        [[comparators]]
        name = "ucx_tcp"
        type = "starway_tcp"

        [comparators.env]
        UCX_TLS = "tcp"

        [[workloads]]
        name = "control_under_load"
        [workloads.matrix]
        control = {{ msg_bytes = 64, warmup_iters = 1, iters = 30 }}
        bulk = {{ duration_s = 0.2, chunk_bytes = "64KiB", inflight = 4, ack_every_n = 4 }}
        """,
    )

    campaign_task = asyncio.create_task(run_campaign(cfg_path, dry_run=False))
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
    await asyncio.wait_for(asyncio.gather(a_task, b_task), timeout=10.0)

    run_root = results_dir / "cluster_smoke_ucx"
    run_dirs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    point_files = list((run_dirs[0] / "points").glob("*.json"))
    assert len(point_files) == 1
    rec = json.loads(point_files[0].read_text(encoding="utf-8"))
    assert rec["status"] == "ok"
    assert "p99_inflation_ratio" in rec.get("metrics", {})

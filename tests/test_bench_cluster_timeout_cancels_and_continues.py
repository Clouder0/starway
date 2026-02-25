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
async def test_cluster_timeout_cancels_and_next_point_still_runs(
    tmp_path: Path,
) -> None:
    from bench.cluster.agent import run_agent
    from bench.run import run_campaign

    results_dir = tmp_path / "results"
    coord_port = _pick_free_port()

    cfg_path = _write_text(
        tmp_path / "cluster.toml",
        f"""
        [campaign]
        id = "cluster_timeout"
        results_dir = "{results_dir}"
        repeats = 1
        seed = 1

        [execution]
        mode = "cluster"
        topology = "two_process"
        point_timeout_s = 1.0

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = {coord_port}
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 34000
        port_range_max = 34100
        agent_connect_timeout_s = 5.0
        agent_rpc_timeout_s = 5.0

        [[comparators]]
        name = "asyncio_tcp"
        type = "asyncio_tcp"

        # First point will exceed point_timeout_s.
        [[workloads]]
        name = "bulk_stream"
        [[workloads.points]]
        chunk_bytes = "64KiB"
        inflight = 4
        ack_every_n = 4
        duration_s = 2.0

        # Second point should still run after the timeout.
        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [10]
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

    run_root = results_dir / "cluster_timeout"
    run_dirs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    point_files = list((run_dirs[0] / "points").glob("*.json"))
    assert len(point_files) == 2

    recs = [json.loads(p.read_text(encoding="utf-8")) for p in point_files]
    by_workload = {r["workload"]: r for r in recs}

    assert by_workload["bulk_stream"]["status"] == "failed"
    assert "timed out" in by_workload["bulk_stream"].get("error", "").lower()
    assert by_workload["control_rtt"]["status"] == "ok"

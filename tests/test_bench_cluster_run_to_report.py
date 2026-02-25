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
async def test_cluster_run_to_report_generates_pdf(tmp_path: Path) -> None:
    from bench.cluster.agent import run_agent
    from bench.report import generate_report
    from bench.run import run_campaign

    results_dir = tmp_path / "results"
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    coord_port = _pick_free_port()

    cfg_path = _write_text(
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

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = {coord_port}
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 32000
        port_range_max = 32100
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
    await asyncio.wait_for(asyncio.gather(a_task, b_task), timeout=5.0)

    run_root = results_dir / "cluster_smoke"
    run_dirs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    run_id = run_dirs[0].name

    spec_path = _write_text(
        paper_dir / "starway-report.toml",
        f"""
        schema_version = 1
        results_roots = ["../results"]
        output_dir = "artifacts/starway_cluster"
        update_current = true

        baseline_runs = []
        new_runs = ["cluster_smoke/{run_id}"]

        main_figures = ["control_rtt_p99"]
        supp_figures = []
        """,
    )

    report_root = generate_report(spec_path)
    assert (report_root / "data" / "points.csv").exists()
    assert (report_root / "tables" / "failures.csv").exists()
    assert (report_root / "figures" / "main" / "fig-control_rtt_p99.pdf").exists()

    # Sanity: points file is ok.
    point_files = list((run_root / run_id / "points").glob("*.json"))
    rec = json.loads(point_files[0].read_text(encoding="utf-8"))
    assert rec["status"] == "ok"

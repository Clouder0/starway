from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_run_campaign_two_process_asyncio_tcp_smoke(tmp_path: Path) -> None:
    results_dir = (tmp_path / "results").as_posix()
    config_text = f"""
[campaign]
id = "test_campaign"
results_dir = "{results_dir}"
repeats = 1

[execution]
mode = "self"
topology = "two_process"
point_timeout_s = 10.0
close_timeout_s = 2.0

[[comparators]]
name = "tcp"
type = "asyncio_tcp"

[comparators.options]
tcp_nodelay = true

[[workloads]]
name = "control_rtt"

[workloads.matrix]
msg_bytes = [8]
warmup_iters = [2]
iters = [10]
""".lstrip()

    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.run import run_campaign

    rc = await run_campaign(path, dry_run=False)
    assert rc == 0


@pytest.mark.asyncio
async def test_run_campaign_two_process_asyncio_tcp_control_under_load_ok(
    tmp_path: Path,
) -> None:
    results_dir = (tmp_path / "results").as_posix()
    config_text = f"""
[campaign]
id = "test_campaign"
results_dir = "{results_dir}"
repeats = 1

[execution]
mode = "self"
topology = "two_process"
point_timeout_s = 10.0
close_timeout_s = 2.0

[[comparators]]
name = "tcp"
type = "asyncio_tcp"

[comparators.options]
tcp_nodelay = true

[[workloads]]
name = "control_under_load"

[workloads.matrix]
control = {{ msg_bytes = 8, warmup_iters = 2, iters = 20 }}
bulk = {{ chunk_bytes = 256, duration_s = 0.05, inflight = 8, ack_every_n = 4, seed = 1 }}
""".lstrip()

    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.run import run_campaign

    rc = await run_campaign(path, dry_run=False)
    assert rc == 0

    campaign_dir = Path(results_dir) / "test_campaign"
    run_dirs = [p for p in campaign_dir.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    run_dir = run_dirs[0]

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest) == 1
    point_id = manifest[0]["point_id"]

    record = json.loads(
        (run_dir / "points" / f"{point_id}.json").read_text(encoding="utf-8")
    )
    assert record["status"] == "ok"
    metrics = record["metrics"]
    assert "control_idle_p99_us" in metrics
    assert "control_loaded_p99_us" in metrics
    assert "p99_inflation_ratio" in metrics
    assert "bulk_goodput_gbps" in metrics

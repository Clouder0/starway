from __future__ import annotations

from pathlib import Path

import pytest


def test_load_config_minimal(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"
results_dir = "results"
repeats = 2

[execution]
mode = "self"

[[comparators]]
name = "asyncio_tcp"
type = "asyncio_tcp"

[[comparators]]
name = "starway_tcp"
type = "starway_tcp"

[comparators.env]
UCX_TLS = "tcp"

[[workloads]]
name = "control_rtt"

[workloads.matrix]
msg_bytes = [64, 1024]
warmup_iters = [10]
iters = [20]
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config

    cfg = load_config(path)
    assert cfg.campaign.id == "test_campaign"
    assert cfg.campaign.repeats == 2
    assert cfg.execution.mode == "self"
    assert [c.name for c in cfg.comparators] == ["asyncio_tcp", "starway_tcp"]
    assert cfg.workloads[0].name == "control_rtt"


def test_load_config_rejects_unknown_mode(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"

[[comparators]]
name = "asyncio_tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_rtt"

[execution]
mode = "nope"
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config

    with pytest.raises(ValueError, match="execution.mode"):
        load_config(path)


def test_plan_manifest_is_deterministic(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"
repeats = 2

[execution]
mode = "self"

[[comparators]]
name = "asyncio_tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_rtt"

[workloads.matrix]
msg_bytes = [64, 1024]
warmup_iters = [10]
iters = [20]
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config
    from bench.planner import plan_manifest

    cfg = load_config(path)
    manifest1 = plan_manifest(cfg)
    manifest2 = plan_manifest(cfg)
    assert len(manifest1) == 4
    assert [p.point_id for p in manifest1] == [p.point_id for p in manifest2]

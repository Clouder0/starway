from __future__ import annotations

from pathlib import Path

import pytest


def test_load_config_parses_execution_topology(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"

[[comparators]]
name = "tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_rtt"

[execution]
mode = "self"
topology = "two_process"
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config

    cfg = load_config(path)
    assert cfg.execution.topology == "two_process"


def test_load_config_rejects_unknown_topology(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"

[[comparators]]
name = "tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_rtt"

[execution]
mode = "self"
topology = "nope"
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config

    with pytest.raises(ValueError, match="execution.topology"):
        load_config(path)


def test_load_config_rejects_inproc_topology(tmp_path: Path) -> None:
    config_text = """
[campaign]
id = "test_campaign"

[[comparators]]
name = "tcp"
type = "asyncio_tcp"

[[workloads]]
name = "control_rtt"

[execution]
mode = "self"
topology = "inproc"
""".lstrip()
    path = tmp_path / "cfg.toml"
    path.write_text(config_text, encoding="utf-8")

    from bench.config import load_config

    with pytest.raises(ValueError, match="inproc"):
        load_config(path)

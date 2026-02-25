import textwrap
from pathlib import Path

import pytest


def _write_config(path: Path, toml_text: str) -> Path:
    path.write_text(textwrap.dedent(toml_text).lstrip(), encoding="utf-8")
    return path


def test_load_config_cluster_parses_cluster_section(tmp_path: Path) -> None:
    from bench.config import load_config

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        """
        [campaign]
        id = "c"

        [execution]
        mode = "cluster"
        topology = "two_process"
        point_timeout_s = 5.0

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = 5001
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 30000
        port_range_max = 30100

        [[comparators]]
        name = "asyncio_tcp"
        type = "asyncio_tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [2]
        """,
    )

    cfg = load_config(cfg_path)
    assert cfg.execution.mode == "cluster"
    assert cfg.cluster is not None
    assert cfg.cluster.coordinator_bind_host == "127.0.0.1"
    assert cfg.cluster.coordinator_port == 5001
    assert cfg.cluster.server_agent_id == "hostA"
    assert cfg.cluster.client_agent_id == "hostB"


def test_load_config_cluster_requires_cluster_table(tmp_path: Path) -> None:
    from bench.config import load_config

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        """
        [campaign]
        id = "c"

        [execution]
        mode = "cluster"
        topology = "two_process"

        [[comparators]]
        name = "asyncio_tcp"
        type = "asyncio_tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [2]
        """,
    )

    with pytest.raises(ValueError, match=r"cluster\."):
        load_config(cfg_path)


def test_load_config_cluster_rejects_non_ipv4_coordinator_host(tmp_path: Path) -> None:
    from bench.config import load_config

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        """
        [campaign]
        id = "c"

        [execution]
        mode = "cluster"
        topology = "two_process"

        [cluster]
        coordinator_bind_host = "localhost"
        coordinator_port = 5001
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 30000
        port_range_max = 30100

        [[comparators]]
        name = "asyncio_tcp"
        type = "asyncio_tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [2]
        """,
    )

    with pytest.raises(ValueError, match=r"coordinator_bind_host"):
        load_config(cfg_path)


def test_load_config_cluster_rejects_shm_py_comparator(tmp_path: Path) -> None:
    from bench.config import load_config

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        """
        [campaign]
        id = "c"

        [execution]
        mode = "cluster"
        topology = "two_process"

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = 5001
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 30000
        port_range_max = 30100

        [[comparators]]
        name = "shm_py"
        type = "shm_py"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [2]
        """,
    )

    with pytest.raises(ValueError, match=r"shm_py"):
        load_config(cfg_path)


def test_load_config_cluster_rejects_starway_ucx_comparator(tmp_path: Path) -> None:
    from bench.config import load_config

    cfg_path = _write_config(
        tmp_path / "cluster.toml",
        """
        [campaign]
        id = "c"

        [execution]
        mode = "cluster"
        topology = "two_process"

        [cluster]
        coordinator_bind_host = "127.0.0.1"
        coordinator_port = 5001
        server_agent_id = "hostA"
        client_agent_id = "hostB"
        port_range_min = 30000
        port_range_max = 30100

        [[comparators]]
        name = "ucx_tcp"
        type = "starway_ucx"
        [comparators.env]
        UCX_TLS = "tcp"

        [[workloads]]
        name = "control_rtt"
        [workloads.matrix]
        msg_bytes = [64]
        warmup_iters = [1]
        iters = [2]
        """,
    )

    with pytest.raises(ValueError, match=r"starway_ucx"):
        load_config(cfg_path)

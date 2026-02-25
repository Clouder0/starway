from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "rel_path",
    [
        "bench/configs/self_transport_matrix_2proc_all_v1.toml",
        "bench/configs/self_transport_matrix_2proc_v2.toml",
    ],
)
def test_transport_matrix_configs_cover_all_workloads(rel_path: str) -> None:
    from bench.config import load_config
    from bench.util import parse_size

    cfg = load_config(Path(rel_path))
    assert cfg.execution.topology == "two_process"

    required = {
        "control_rtt",
        "small_message_pressure",
        "bulk_stream",
        "mixed_size",
        "bucket_duplex_step",
        "control_under_load",
    }
    workloads = {w.name for w in cfg.workloads}
    assert required.issubset(workloads)

    by_name = {w.name: w for w in cfg.workloads}

    # Ensure figures won't be degenerate (multiple x values).
    control = by_name["control_rtt"].matrix.get("msg_bytes")
    assert isinstance(control, list)
    assert len(control) >= 2

    smp = by_name["small_message_pressure"].matrix.get("msg_bytes")
    assert isinstance(smp, list)
    assert len(smp) >= 2

    bulk_pts = by_name["bulk_stream"].points
    inflight_to_chunks: dict[int, set[int]] = {}
    for p in bulk_pts:
        inflight = int(p["inflight"])
        chunk_b = int(parse_size(p["chunk_bytes"]))
        inflight_to_chunks.setdefault(inflight, set()).add(chunk_b)
    assert inflight_to_chunks, "bulk_stream must define explicit points"
    for inflight, chunks in inflight_to_chunks.items():
        assert len(chunks) >= 2, (
            f"bulk_stream inflight={inflight} must have >=2 chunk sizes"
        )

    mixed_pts = by_name["mixed_size"].points
    assert len(mixed_pts) >= 2
    mixed_maxes: set[int] = set()
    for p in mixed_pts:
        sizes = p.get("sizes")
        assert isinstance(sizes, list)
        mixed_maxes.add(max(int(parse_size(s)) for s in sizes) if sizes else 0)
    assert len(mixed_maxes) >= 2

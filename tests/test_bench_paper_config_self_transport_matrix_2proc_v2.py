from __future__ import annotations

from pathlib import Path


def test_paper_config_self_transport_matrix_2proc_v2_bulk_stream_not_degenerate() -> (
    None
):
    cfg_path = (
        Path(__file__).resolve().parents[1]
        / "bench"
        / "configs"
        / "self_transport_matrix_2proc_v2.toml"
    )

    from bench.config import load_config
    from bench.planner import plan_manifest
    from bench.util import parse_size

    cfg = load_config(cfg_path)
    assert cfg.execution.topology == "two_process"

    manifest = plan_manifest(cfg)
    bulk = [p for p in manifest if p.workload == "bulk_stream"]
    assert bulk, "config must include bulk_stream points"

    chunks_by_inflight: dict[int, set[int]] = {}
    for p in bulk:
        inflight = int(p.params.get("inflight", 0))
        chunk_raw = p.params.get("chunk_bytes")
        if isinstance(chunk_raw, (int, float)):
            chunk_b = int(chunk_raw)
        else:
            chunk_b = int(parse_size(chunk_raw))
        chunks_by_inflight.setdefault(inflight, set()).add(chunk_b)

    assert chunks_by_inflight, "bulk_stream points must define inflight"
    for inflight, chunks in chunks_by_inflight.items():
        assert inflight > 0
        assert len(chunks) >= 2, (
            f"inflight={inflight} has only {len(chunks)} chunk sizes"
        )

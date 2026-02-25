from __future__ import annotations

import csv
from pathlib import Path


def test_write_points_csv_has_stable_core_columns_first(tmp_path: Path) -> None:
    from bench.report import write_points_csv

    rows = [
        {
            "condition": "baseline",
            "run_id": "r1",
            "campaign_id": "c",
            "comparator": "asyncio_tcp",
            "workload": "control_rtt",
            "repeat": 1,
            "point_id": "p1",
            "status": "ok",
            "param.msg_bytes": 64,
            "metric.rtt_p99_us": 500.0,
        },
        {
            "condition": "new",
            "run_id": "r2",
            "campaign_id": "c",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "repeat": 1,
            "point_id": "p2",
            "status": "ok",
            "param.chunk_bytes": "4MiB",
            "param.chunk_bytes_B": 4 * 1024 * 1024,
            "metric.goodput_gbps": 12.3,
        },
    ]

    out_path = tmp_path / "data" / "points.csv"
    write_points_csv(rows, out_path)

    with out_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

    assert header[:8] == [
        "condition",
        "run_id",
        "campaign_id",
        "comparator",
        "workload",
        "repeat",
        "point_id",
        "status",
    ]
    assert "param.msg_bytes" in header
    assert "param.chunk_bytes" in header
    assert "metric.rtt_p99_us" in header
    assert "metric.goodput_gbps" in header

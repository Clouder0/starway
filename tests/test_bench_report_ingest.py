from __future__ import annotations

import csv
import json
from pathlib import Path


def _write_run_dir(run_dir: Path, *, run_id: str) -> None:
    (run_dir / "points").mkdir(parents=True)
    (run_dir / "run_meta.json").write_text(
        json.dumps({"campaign_id": "c", "run_id": run_id}), encoding="utf-8"
    )


def test_build_rows_for_run_adds_run_id_and_size_bytes(tmp_path: Path) -> None:
    from bench.report import build_rows_for_run

    run_dir = tmp_path / "results" / "c" / "r1"
    _write_run_dir(run_dir, run_id="r1")
    (run_dir / "points" / "p1.json").write_text(
        json.dumps(
            {
                "campaign_id": "c",
                "point_id": "p1",
                "comparator": "starway_tcp",
                "workload": "bulk_stream",
                "repeat": 1,
                "status": "ok",
                "params": {"chunk_bytes": "4MiB", "inflight": 16},
                "metrics": {"goodput_gbps": 2.5},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    rows = build_rows_for_run(run_dir, condition="baseline")
    assert len(rows) == 1
    row = rows[0]
    assert row["condition"] == "baseline"
    assert row["run_id"] == "r1"
    assert row["param.chunk_bytes"] == "4MiB"
    assert row["param.chunk_bytes_B"] == 4 * 1024 * 1024


def test_write_failures_csv_contains_failed_points(tmp_path: Path) -> None:
    from bench.report import write_failures_csv

    rows = [
        {
            "condition": "baseline",
            "run_id": "r1",
            "point_id": "ok",
            "workload": "control_rtt",
            "comparator": "asyncio_tcp",
            "repeat": 1,
            "status": "ok",
        },
        {
            "condition": "baseline",
            "run_id": "r1",
            "point_id": "bad",
            "workload": "control_rtt",
            "comparator": "starway_tcp",
            "repeat": 1,
            "status": "failed",
            "error": "boom",
        },
    ]
    out_path = tmp_path / "tables" / "failures.csv"
    write_failures_csv(rows, out_path)

    with out_path.open("r", encoding="utf-8", newline="") as f:
        got = list(csv.DictReader(f))
    assert len(got) == 1
    assert got[0]["point_id"] == "bad"
    assert got[0]["status"] == "failed"
    assert got[0]["error"] == "boom"

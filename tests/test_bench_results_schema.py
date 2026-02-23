from __future__ import annotations

import json
from pathlib import Path

from bench.results import ResultWriter
from bench.types import ManifestPoint


def test_result_writer_writes_expected_files(tmp_path: Path) -> None:
    base = tmp_path / "results" / "campaign"
    writer = ResultWriter(base)

    writer.write_run_meta({"campaign_id": "campaign", "config_hash": "abc"})
    writer.write_manifest(
        [
            ManifestPoint(
                point_id="p1",
                comparator="asyncio_tcp",
                workload="control_rtt",
                repeat=1,
                params={"msg_bytes": 64},
            )
        ]
    )
    writer.log_event({"type": "run_start"})
    writer.write_point(
        "p1",
        {
            "status": "ok",
            "metrics": {"rtt_p50_us": 1.0},
            "config_snapshot": {"workload": "control_rtt"},
        },
    )

    meta = json.loads((base / "run_meta.json").read_text(encoding="utf-8"))
    assert meta["campaign_id"] == "campaign"
    assert (base / "manifest.json").exists()
    assert (base / "events.jsonl").exists()
    point = json.loads((base / "points" / "p1.json").read_text(encoding="utf-8"))
    assert point["status"] == "ok"
    assert "metrics" in point

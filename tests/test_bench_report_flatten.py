from __future__ import annotations

import json


def test_flatten_params_and_metrics_nested_dicts() -> None:
    from bench.report import flatten_point_record

    record = {
        "campaign_id": "c",
        "point_id": "p",
        "comparator": "starway_tcp",
        "workload": "control_under_load",
        "repeat": 1,
        "status": "ok",
        "params": {
            "bulk": {"chunk_bytes": "4MiB", "inflight": 16},
            "control": {"msg_bytes": 64},
            "seed": 123,
        },
        "metrics": {"bulk_goodput_gbps": 26.9, "p99_inflation_ratio": 1.5},
    }

    row = flatten_point_record(
        record,
        condition="baseline",
        run_id="r1",
    )
    assert row["condition"] == "baseline"
    assert row["run_id"] == "r1"
    assert row["workload"] == "control_under_load"
    assert row["param.bulk.chunk_bytes"] == "4MiB"
    assert row["param.bulk.inflight"] == 16
    assert row["param.control.msg_bytes"] == 64
    assert row["param.seed"] == 123
    assert row["metric.bulk_goodput_gbps"] == 26.9


def test_flatten_stores_lists_as_canonical_json() -> None:
    from bench.report import flatten_point_record

    record = {
        "campaign_id": "c",
        "point_id": "p",
        "comparator": "asyncio_tcp",
        "workload": "mixed_size",
        "repeat": 1,
        "status": "ok",
        "params": {"sizes": ["64B", "1KiB", "64KiB"]},
        "metrics": {"goodput_gbps": 1.23},
    }

    row = flatten_point_record(record, condition="new", run_id="r2")
    assert row["param.sizes"] == json.dumps(
        ["64B", "1KiB", "64KiB"], separators=(",", ":"), sort_keys=True
    )

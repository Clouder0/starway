from __future__ import annotations


def test_aggregate_rows_median_and_counts() -> None:
    from bench.report import aggregate_rows

    rows = [
        {
            "condition": "baseline",
            "run_id": "r1",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "status": "ok",
            "param.chunk_bytes": "4MiB",
            "param.inflight": 16,
            "param.seed": 1,
            "metric.goodput_gbps": 10.0,
        },
        {
            "condition": "baseline",
            "run_id": "r2",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "status": "ok",
            "param.chunk_bytes": "4MiB",
            "param.inflight": 16,
            "param.seed": 2,
            "metric.goodput_gbps": 14.0,
        },
        {
            "condition": "baseline",
            "run_id": "r3",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "status": "failed",
            "param.chunk_bytes": "4MiB",
            "param.inflight": 16,
            "param.seed": 3,
            "error": "boom",
        },
    ]

    agg = aggregate_rows(rows)
    assert len(agg) == 1
    row = agg[0]
    assert row["condition"] == "baseline"
    assert row["comparator"] == "starway_tcp"
    assert row["workload"] == "bulk_stream"
    assert row["param.chunk_bytes"] == "4MiB"
    assert row["param.inflight"] == 16
    assert row["n_total"] == 3
    assert row["n_ok"] == 2
    assert row["metric.goodput_gbps"] == 12.0

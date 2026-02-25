from __future__ import annotations

from pathlib import Path


def _assert_is_pdf(path: Path) -> None:
    assert path.exists(), f"missing file: {path}"
    header = path.read_bytes()[:4]
    assert header == b"%PDF"


def test_render_control_rtt_p99_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "control_rtt",
            "param.msg_bytes": 64,
            "metric.rtt_p99_us": 500.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "control_rtt",
            "param.msg_bytes": 1024,
            "metric.rtt_p99_us": 520.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "new",
            "comparator": "starway_tcp",
            "workload": "control_rtt",
            "param.msg_bytes": 64,
            "metric.rtt_p99_us": 300.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "new",
            "comparator": "starway_tcp",
            "workload": "control_rtt",
            "param.msg_bytes": 1024,
            "metric.rtt_p99_us": 310.0,
            "n_ok": 1,
            "n_total": 1,
        },
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("control_rtt_p99", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_render_bulk_stream_goodput_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "baseline",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "param.chunk_bytes": "1MiB",
            "param.inflight": 16,
            "metric.goodput_gbps": 10.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "baseline",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "param.chunk_bytes": "4MiB",
            "param.inflight": 16,
            "metric.goodput_gbps": 20.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "new",
            "comparator": "starway_tcp",
            "workload": "bulk_stream",
            "param.chunk_bytes": "1MiB",
            "param.inflight": 16,
            "metric.goodput_gbps": 12.0,
            "n_ok": 1,
            "n_total": 1,
        },
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("bulk_stream_goodput", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_render_bucket_duplex_step_aggregate_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "bucket_duplex_step",
            "param.bucket_bytes": "1MiB",
            "metric.aggregate_gbps": 5.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "bucket_duplex_step",
            "param.bucket_bytes": "4MiB",
            "metric.aggregate_gbps": 8.0,
            "n_ok": 1,
            "n_total": 1,
        },
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("bucket_duplex_step_aggregate", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_render_control_under_load_inflation_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "new",
            "comparator": "starway_tcp",
            "workload": "control_under_load",
            "metric.bulk_goodput_gbps": 25.0,
            "metric.p99_inflation_ratio": 1.4,
            "n_ok": 1,
            "n_total": 1,
        }
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("control_under_load_inflation", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_render_small_message_pressure_goodput_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "small_message_pressure",
            "param.msg_bytes": 64,
            "metric.goodput_gbps": 2.5,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "small_message_pressure",
            "param.msg_bytes": 1024,
            "metric.goodput_gbps": 5.0,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "new",
            "comparator": "asyncio_tcp",
            "workload": "small_message_pressure",
            "param.msg_bytes": 64,
            "metric.goodput_gbps": 3.0,
            "n_ok": 1,
            "n_total": 1,
        },
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("small_message_pressure_goodput", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_render_mixed_size_goodput_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    rows = [
        {
            "condition": "baseline",
            "comparator": "asyncio_tcp",
            "workload": "mixed_size",
            "param.sizes": '["64B","1KiB","64KiB"]',
            "metric.goodput_gbps": 1.2,
            "n_ok": 1,
            "n_total": 1,
        },
        {
            "condition": "new",
            "comparator": "asyncio_tcp",
            "workload": "mixed_size",
            "param.sizes": '["64B","1KiB","64KiB"]',
            "metric.goodput_gbps": 1.5,
            "n_ok": 1,
            "n_total": 1,
        },
    ]

    out_dir = tmp_path / "figures"
    art = render_figure("mixed_size_goodput", rows, out_dir)
    _assert_is_pdf(art.pdf_path)


def test_format_bytes_binary_human_readable() -> None:
    from bench.report_figures import format_bytes_binary

    assert format_bytes_binary(0) == "0B"
    assert format_bytes_binary(1) == "1B"
    assert format_bytes_binary(64) == "64B"
    assert format_bytes_binary(1024) == "1KiB"
    assert format_bytes_binary(64 * 1024) == "64KiB"
    assert format_bytes_binary(1024 * 1024) == "1MiB"
    assert format_bytes_binary(16 * 1024 * 1024) == "16MiB"


def test_render_bulk_stream_goodput_many_chunk_sizes_writes_pdf(tmp_path: Path) -> None:
    from bench.report_figures import render_figure

    chunk_sizes = [2**p for p in range(10, 25, 2)]  # 1KiB .. 16MiB
    rows: list[dict] = []
    for chunk in chunk_sizes:
        rows.append(
            {
                "condition": "new",
                "comparator": "starway_tcp",
                "workload": "bulk_stream",
                "param.chunk_bytes": chunk,
                "param.inflight": 16,
                "metric.goodput_gbps": float(chunk) / 1e6,
                "n_ok": 1,
                "n_total": 1,
            }
        )

    out_dir = tmp_path / "figures"
    art = render_figure("bulk_stream_goodput", rows, out_dir)
    _assert_is_pdf(art.pdf_path)

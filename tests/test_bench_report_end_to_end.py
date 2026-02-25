from __future__ import annotations

import json
from pathlib import Path


def _write_run_dir(run_dir: Path, *, run_id: str, point_records: list[dict]) -> None:
    (run_dir / "points").mkdir(parents=True)
    (run_dir / "run_meta.json").write_text(
        json.dumps({"campaign_id": "c", "run_id": run_id}), encoding="utf-8"
    )
    (run_dir / "manifest.json").write_text(
        json.dumps({}, sort_keys=True), encoding="utf-8"
    )
    for rec in point_records:
        (run_dir / "points" / f"{rec['point_id']}.json").write_text(
            json.dumps(rec, sort_keys=True), encoding="utf-8"
        )


def test_generate_report_writes_paper_pack_and_current_symlink(tmp_path: Path) -> None:
    from bench.report import generate_report

    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    results_root = tmp_path / "results"

    baseline_run = results_root / "baseline_tcp_self_v1" / "r_baseline"
    new_run = results_root / "baseline_tcp_self_v1" / "r_new"

    _write_run_dir(
        baseline_run,
        run_id="r_baseline",
        point_records=[
            {
                "campaign_id": "c",
                "point_id": "p1",
                "comparator": "asyncio_tcp",
                "workload": "control_rtt",
                "repeat": 1,
                "status": "ok",
                "params": {"msg_bytes": 64},
                "metrics": {"rtt_p99_us": 500.0},
            }
        ],
    )
    _write_run_dir(
        new_run,
        run_id="r_new",
        point_records=[
            {
                "campaign_id": "c",
                "point_id": "p2",
                "comparator": "asyncio_tcp",
                "workload": "control_rtt",
                "repeat": 1,
                "status": "ok",
                "params": {"msg_bytes": 64},
                "metrics": {"rtt_p99_us": 450.0},
            }
        ],
    )

    spec_path = paper_dir / "starway-report.toml"
    spec_path.write_text(
        "\n".join(
            [
                "schema_version = 1",
                'results_roots = ["../results"]',
                'output_dir = "artifacts/starway"',
                "update_current = true",
                'baseline_runs = ["baseline_tcp_self_v1/r_baseline"]',
                'new_runs = ["baseline_tcp_self_v1/r_new"]',
                'main_figures = ["control_rtt_p99"]',
                "supp_figures = []",
                "",
            ]
        ),
        encoding="utf-8",
    )

    report_root = generate_report(spec_path)
    assert report_root.exists()
    assert (report_root / "data" / "points.csv").exists()
    assert (report_root / "tables" / "failures.csv").exists()
    assert (report_root / "meta" / "report_meta.json").exists()
    assert (report_root / "figures" / "main" / "fig-control_rtt_p99.pdf").exists()
    pack = (report_root / "typst" / "pack.typ").read_text(encoding="utf-8")
    assert "fig_control_rtt_p99" in pack

    current = paper_dir / "artifacts" / "starway" / "current"
    assert current.is_symlink()
    assert current.resolve() == report_root

from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_min_run_dir(run_dir: Path) -> None:
    (run_dir / "points").mkdir(parents=True)
    (run_dir / "run_meta.json").write_text(
        json.dumps({"campaign_id": "c", "run_id": run_dir.name}), encoding="utf-8"
    )


def test_load_report_spec_resolves_paths(tmp_path: Path) -> None:
    # Simulate a paper repo with a single TOML spec file.
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()

    # Simulate a results tree produced by the bench runner.
    results_root = tmp_path / "results"
    run_dir = results_root / "baseline_tcp_self_v1" / "r1"
    _write_min_run_dir(run_dir)

    spec_path = paper_dir / "starway-report.toml"
    spec_path.write_text(
        "\n".join(
            [
                "schema_version = 1",
                'results_roots = ["../results"]',
                'output_dir = "artifacts/starway"',
                "update_current = true",
                'baseline_runs = ["baseline_tcp_self_v1/r1"]',
                "new_runs = []",
                'main_figures = ["control_rtt_p99"]',
                "supp_figures = []",
                "",
            ]
        ),
        encoding="utf-8",
    )

    from bench.report import load_report_spec

    spec = load_report_spec(spec_path)
    assert spec.schema_version == 1
    assert spec.output_dir == (paper_dir / "artifacts" / "starway").resolve()
    assert spec.baseline_run_dirs == [run_dir.resolve()]
    assert spec.new_run_dirs == []


def test_load_report_spec_raises_for_missing_run(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    (tmp_path / "results").mkdir()

    spec_path = paper_dir / "starway-report.toml"
    spec_path.write_text(
        "\n".join(
            [
                "schema_version = 1",
                'results_roots = ["../results"]',
                'output_dir = "artifacts/starway"',
                "update_current = true",
                'baseline_runs = ["does_not_exist/run1"]',
                "new_runs = []",
                "main_figures = []",
                "supp_figures = []",
                "",
            ]
        ),
        encoding="utf-8",
    )

    from bench.report import load_report_spec

    with pytest.raises(ValueError, match="Could not resolve run"):
        load_report_spec(spec_path)


def test_load_report_spec_rejects_unknown_schema_version(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    (tmp_path / "results").mkdir()

    spec_path = paper_dir / "starway-report.toml"
    spec_path.write_text(
        "\n".join(
            [
                "schema_version = 999",
                'results_roots = ["../results"]',
                'output_dir = "artifacts/starway"',
                "update_current = false",
                "baseline_runs = []",
                "new_runs = []",
                "main_figures = []",
                "supp_figures = []",
                "",
            ]
        ),
        encoding="utf-8",
    )

    from bench.report import load_report_spec

    with pytest.raises(ValueError, match="schema_version"):
        load_report_spec(spec_path)

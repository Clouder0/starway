from __future__ import annotations

import dataclasses
from dataclasses import dataclass
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .util import parse_size

if TYPE_CHECKING:  # pragma: no cover
    from .report_figures import FigureArtifact


@dataclass(frozen=True)
class ReportSpec:
    """Resolved report specification.

    Paths are resolved relative to the spec file location and converted to
    absolute paths.
    """

    schema_version: int
    spec_path: Path
    results_roots: list[Path]
    output_dir: Path
    update_current: bool
    baseline_run_dirs: list[Path]
    new_run_dirs: list[Path]
    main_figures: list[str]
    supp_figures: list[str]


def _expect_type(value: Any, typ: type, *, field: str) -> Any:
    if not isinstance(value, typ):
        raise ValueError(f"{field} must be {typ.__name__}, got {type(value).__name__}")
    return value


def _expect_list_str(value: Any, *, field: str) -> list[str]:
    value = _expect_type(value, list, field=field)
    out: list[str] = []
    for i, v in enumerate(value):
        if not isinstance(v, str):
            raise ValueError(f"{field}[{i}] must be str, got {type(v).__name__}")
        out.append(v)
    return out


def _resolve_path(base_dir: Path, raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = base_dir / p
    return p.resolve()


def _is_run_dir(path: Path) -> bool:
    return (path / "run_meta.json").exists() and (path / "points").is_dir()


def _resolve_run_dir(*, results_roots: list[Path], run_ref: str) -> Path:
    raw = Path(run_ref).expanduser()

    # Absolute paths are allowed.
    if raw.is_absolute():
        candidate = raw.resolve()
        if _is_run_dir(candidate):
            return candidate
        raise ValueError(
            f"Could not resolve run '{run_ref}': not a run dir (missing run_meta.json/points/)"
        )

    for root in results_roots:
        candidate = (root / raw).resolve()
        if _is_run_dir(candidate):
            return candidate

    roots_txt = ", ".join(str(r) for r in results_roots)
    raise ValueError(
        f"Could not resolve run '{run_ref}' under results_roots: {roots_txt}"
    )


def load_report_spec(spec_path: Path) -> ReportSpec:
    """Load and resolve a report TOML spec.

    The returned object contains absolute, resolved paths.
    """

    spec_path = Path(spec_path).expanduser().resolve()
    spec_dir = spec_path.parent

    try:
        import tomllib  # py311+
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("tomllib is required (python>=3.11)") from exc

    raw_doc = tomllib.loads(spec_path.read_text(encoding="utf-8"))
    if not isinstance(raw_doc, dict):
        raise ValueError("TOML document must be a table")

    schema_version = int(raw_doc.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported schema_version={schema_version} (expected 1)")

    results_roots_raw = _expect_list_str(
        raw_doc.get("results_roots"), field="results_roots"
    )
    results_roots = [_resolve_path(spec_dir, r) for r in results_roots_raw]
    if not results_roots:
        raise ValueError("results_roots must be non-empty")

    output_dir_raw = _expect_type(raw_doc.get("output_dir"), str, field="output_dir")
    output_dir = _resolve_path(spec_dir, output_dir_raw)

    update_current = bool(raw_doc.get("update_current", True))

    baseline_runs = _expect_list_str(
        raw_doc.get("baseline_runs", []), field="baseline_runs"
    )
    new_runs = _expect_list_str(raw_doc.get("new_runs", []), field="new_runs")
    main_figures = _expect_list_str(
        raw_doc.get("main_figures", []), field="main_figures"
    )
    supp_figures = _expect_list_str(
        raw_doc.get("supp_figures", []), field="supp_figures"
    )

    baseline_run_dirs = [
        _resolve_run_dir(results_roots=results_roots, run_ref=r) for r in baseline_runs
    ]
    new_run_dirs = [
        _resolve_run_dir(results_roots=results_roots, run_ref=r) for r in new_runs
    ]

    # Stable ordering (and also stable output diffs).
    baseline_run_dirs = sorted(dict.fromkeys(baseline_run_dirs))
    new_run_dirs = sorted(dict.fromkeys(new_run_dirs))
    main_figures = list(dict.fromkeys(main_figures))
    supp_figures = list(dict.fromkeys(supp_figures))

    return ReportSpec(
        schema_version=schema_version,
        spec_path=spec_path,
        results_roots=results_roots,
        output_dir=output_dir,
        update_current=update_current,
        baseline_run_dirs=baseline_run_dirs,
        new_run_dirs=new_run_dirs,
        main_figures=main_figures,
        supp_figures=supp_figures,
    )


def _flatten_mapping(
    out: dict[str, Any], mapping: dict[str, Any], *, prefix: str
) -> None:
    """Flatten nested dictionaries into dotted keys.

    Lists/tuples are stored as canonical JSON strings (to keep the intermediate
    dataset stable without exploding into many columns).
    """

    for k, v in mapping.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            _flatten_mapping(out, v, prefix=f"{key}.")
        elif isinstance(v, (list, tuple)):
            out[key] = json.dumps(v, separators=(",", ":"), sort_keys=True, default=str)
        else:
            out[key] = v


def flatten_point_record(
    record: dict[str, Any],
    *,
    condition: str,
    run_id: str,
) -> dict[str, Any]:
    """Convert a point JSON record into a flat, CSV-friendly row."""

    out: dict[str, Any] = {
        "condition": condition,
        "run_id": run_id,
    }

    for key in (
        "campaign_id",
        "point_id",
        "comparator",
        "workload",
        "repeat",
        "status",
        "error",
        "elapsed_s",
        "started_ts",
        "ended_ts",
    ):
        if key in record:
            out[key] = record[key]

    params = record.get("params")
    if isinstance(params, dict):
        _flatten_mapping(out, params, prefix="param.")

    metrics = record.get("metrics")
    if isinstance(metrics, dict):
        _flatten_mapping(out, metrics, prefix="metric.")

    return out


def build_rows_for_run(run_dir: Path, *, condition: str) -> list[dict[str, Any]]:
    """Load a single run directory into normalized point rows."""

    run_dir = Path(run_dir).expanduser().resolve()
    run_meta_path = run_dir / "run_meta.json"
    if not run_meta_path.exists():
        raise ValueError(f"run_meta.json not found: {run_meta_path}")

    run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    if not isinstance(run_meta, dict):
        raise ValueError(f"run_meta.json must be an object: {run_meta_path}")

    run_id_val = run_meta.get("run_id")
    run_id = str(run_id_val) if isinstance(run_id_val, (str, int)) else run_dir.name

    points_dir = run_dir / "points"
    if not points_dir.is_dir():
        raise ValueError(f"points/ directory not found: {points_dir}")

    point_files = sorted(p for p in points_dir.iterdir() if p.suffix == ".json")
    rows: list[dict[str, Any]] = []
    for path in point_files:
        record = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(record, dict):
            raise ValueError(f"point record must be an object: {path}")
        row = flatten_point_record(record, condition=condition, run_id=run_id)

        # Add helper numeric columns for byte-size parameters.
        for k, v in list(row.items()):
            if not k.startswith("param."):
                continue
            if not isinstance(v, str):
                continue
            if v.startswith("[") or v.startswith("{"):
                continue
            if not k.rsplit(".", 1)[-1].endswith("bytes"):
                continue
            try:
                row[f"{k}_B"] = int(parse_size(v))
            except Exception:
                # Not a parseable size string.
                continue

        rows.append(row)

    return rows


def write_failures_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    import csv

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    failures = [r for r in rows if r.get("status") != "ok"]
    fieldnames = [
        "condition",
        "run_id",
        "campaign_id",
        "comparator",
        "workload",
        "repeat",
        "point_id",
        "status",
        "error",
    ]

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in failures:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _csv_field_order(all_keys: set[str]) -> list[str]:
    core = [
        "condition",
        "run_id",
        "campaign_id",
        "comparator",
        "workload",
        "repeat",
        "point_id",
        "status",
        "error",
        "elapsed_s",
        "started_ts",
        "ended_ts",
    ]
    core_set = set(core)
    extra = [k for k in all_keys if k not in core_set]

    def sort_key(k: str) -> tuple[int, str]:
        if k.startswith("param."):
            return (0, k)
        if k.startswith("metric."):
            return (1, k)
        return (2, k)

    return core + sorted(extra, key=sort_key)


def write_points_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    import csv

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_keys: set[str] = set()
    for r in rows:
        all_keys.update(r.keys())

    fieldnames = _csv_field_order(all_keys)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def aggregate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate replicate points (across runs) by median.

    Grouping key:
    - condition, comparator, workload
    - all `param.*` keys except seeds and derived helper columns (e.g. `*_B`)

    Output includes `n_total` and `n_ok` and aggregates `metric.*` keys across ok rows.
    """

    import statistics

    groups: dict[tuple[tuple[str, Any], ...], list[dict[str, Any]]] = {}

    for r in rows:
        condition = r.get("condition")
        comparator = r.get("comparator")
        workload = r.get("workload")

        # Collect stable param key/value pairs.
        params: list[tuple[str, Any]] = []
        for k, v in r.items():
            if not k.startswith("param."):
                continue
            if k.endswith("_B"):
                continue
            if k.rsplit(".", 1)[-1] == "seed":
                continue
            params.append((k, v))
        params.sort(key=lambda kv: kv[0])

        key = (
            ("condition", condition),
            ("comparator", comparator),
            ("workload", workload),
            *params,
        )
        groups.setdefault(key, []).append(r)

    out_rows: list[dict[str, Any]] = []
    for key, members in groups.items():
        out: dict[str, Any] = {}
        for k, v in key:
            out[k] = v

        out["n_total"] = len(members)
        ok = [m for m in members if m.get("status") == "ok"]
        out["n_ok"] = len(ok)

        metric_keys: set[str] = set()
        for m in ok:
            for k in m.keys():
                if k.startswith("metric."):
                    metric_keys.add(k)

        for mk in sorted(metric_keys):
            values: list[float] = []
            for m in ok:
                v = m.get(mk)
                if isinstance(v, (int, float)):
                    values.append(float(v))
            if values:
                out[mk] = statistics.median(values)

        out_rows.append(out)

    def stable_sort_key(r: dict[str, Any]) -> tuple[str, str, str, str]:
        # Param keys are already present in dict; sorting by their repr is good enough
        # for deterministic ordering in reports.
        return (
            str(r.get("condition", "")),
            str(r.get("comparator", "")),
            str(r.get("workload", "")),
            repr(sorted((k, r[k]) for k in r.keys() if k.startswith("param."))),
        )

    return sorted(out_rows, key=stable_sort_key)


def _compute_report_id(spec_path: Path) -> str:
    import hashlib

    spec_path = Path(spec_path)
    h = hashlib.sha256(spec_path.read_bytes()).hexdigest()[:10]
    return f"{spec_path.stem}-{h}"


def _escape_typst_string(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _write_typst_pack(
    *,
    pack_path: Path,
    main_figures: list[FigureArtifact],
    supp_figures: list[FigureArtifact],
) -> None:
    import os

    pack_path = Path(pack_path)
    pack_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("// Generated by starway bench report tool")
    lines.append("// Do not edit by hand.")
    lines.append("")
    lines.append("#let sw_fig = (path, caption, alt: none) => {")
    lines.append("  figure(")
    lines.append("    image(path, width: 100%, alt: alt),")
    lines.append("    caption: caption,")
    lines.append("  )")
    lines.append("}")
    lines.append("")

    def emit(art: FigureArtifact) -> None:
        rel = os.path.relpath(art.pdf_path, start=pack_path.parent)
        rel = rel.replace(os.sep, "/")
        macro = f"fig_{art.figure_id}"
        caption = art.caption
        alt = _escape_typst_string(art.alt)
        lines.append(
            f'#let {macro} = () => sw_fig("{rel}", [{caption}], alt: "{alt}") <{art.label}>'
        )

    # Note: pack.typ lives under typst/, so pdf paths should resolve relative to it.
    # We keep ordering stable and matching the spec.
    for art in main_figures:
        emit(art)
    for art in supp_figures:
        emit(art)

    pack_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_run_meta_for_report(run_dir: Path) -> dict[str, Any]:
    run_meta_path = Path(run_dir) / "run_meta.json"
    try:
        meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"run_dir": str(run_dir), "error": "missing run_meta.json"}
    if not isinstance(meta, dict):
        return {"run_dir": str(run_dir), "error": "run_meta.json is not an object"}
    meta = dict(meta)
    meta["run_dir"] = str(Path(run_dir).resolve())
    return meta


def generate_report(spec_path: Path) -> Path:
    """Generate a complete paper pack under the spec's output_dir."""

    import os
    import sys

    from .report_figures import FigureArtifact, render_figure

    spec = load_report_spec(spec_path)
    report_id = _compute_report_id(spec.spec_path)
    report_root = (spec.output_dir / report_id).resolve()

    # Layout.
    data_dir = report_root / "data"
    tables_dir = report_root / "tables"
    figures_main_dir = report_root / "figures" / "main"
    figures_supp_dir = report_root / "figures" / "supp"
    typst_dir = report_root / "typst"
    meta_dir = report_root / "meta"
    for d in (
        data_dir,
        tables_dir,
        figures_main_dir,
        figures_supp_dir,
        typst_dir,
        meta_dir,
    ):
        d.mkdir(parents=True, exist_ok=True)

    # Ingest.
    rows: list[dict[str, Any]] = []
    for run_dir in spec.baseline_run_dirs:
        rows.extend(build_rows_for_run(run_dir, condition="baseline"))
    for run_dir in spec.new_run_dirs:
        rows.extend(build_rows_for_run(run_dir, condition="new"))

    write_points_csv(rows, data_dir / "points.csv")
    write_failures_csv(rows, tables_dir / "failures.csv")

    agg_rows = aggregate_rows(rows)

    # Figures.
    main_arts: list[FigureArtifact] = []
    for fig_id in spec.main_figures:
        main_arts.append(render_figure(fig_id, agg_rows, figures_main_dir))
    supp_arts: list[FigureArtifact] = []
    for fig_id in spec.supp_figures:
        supp_arts.append(render_figure(fig_id, agg_rows, figures_supp_dir))

    _write_typst_pack(
        pack_path=typst_dir / "pack.typ",
        main_figures=main_arts,
        supp_figures=supp_arts,
    )

    # Metadata.
    try:
        import matplotlib

        mpl_version = matplotlib.__version__
    except Exception:
        mpl_version = "unknown"

    report_meta = {
        "report_id": report_id,
        "spec_path": str(spec.spec_path),
        "baseline_run_dirs": [str(p) for p in spec.baseline_run_dirs],
        "new_run_dirs": [str(p) for p in spec.new_run_dirs],
        "figures": {
            "main": [
                {
                    "figure_id": a.figure_id,
                    "pdf_path": str(a.pdf_path.relative_to(report_root)),
                    "label": a.label,
                }
                for a in main_arts
            ],
            "supp": [
                {
                    "figure_id": a.figure_id,
                    "pdf_path": str(a.pdf_path.relative_to(report_root)),
                    "label": a.label,
                }
                for a in supp_arts
            ],
        },
        "tool": {
            "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "matplotlib": mpl_version,
        },
        "runs": {
            "baseline": [_load_run_meta_for_report(p) for p in spec.baseline_run_dirs],
            "new": [_load_run_meta_for_report(p) for p in spec.new_run_dirs],
        },
    }
    (meta_dir / "report_meta.json").write_text(
        json.dumps(report_meta, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    # Update current symlink.
    if spec.update_current:
        current = spec.output_dir / "current"
        current.parent.mkdir(parents=True, exist_ok=True)
        if current.exists() or current.is_symlink():
            if current.is_symlink() or current.is_file():
                current.unlink()
            else:
                raise ValueError(
                    f"Refusing to replace non-file current path: {current}"
                )
        os.symlink(report_id, current)

    return report_root


def _asdict(obj: Any) -> Any:
    # dataclasses.is_dataclass() is True for both instances and dataclass types.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    return obj


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Starway bench report generator")
    parser.add_argument("spec", type=Path, help="Path to starway-report.toml")
    parser.add_argument(
        "--print-resolved",
        action="store_true",
        help="Print resolved spec as JSON and exit.",
    )
    args = parser.parse_args(argv)

    spec = load_report_spec(args.spec)
    if args.print_resolved:
        payload = dataclasses.asdict(spec)
        # Convert paths to strings for JSON printing.
        for k, v in list(payload.items()):
            if isinstance(v, Path):
                payload[k] = str(v)
            if isinstance(v, list) and v and isinstance(v[0], Path):
                payload[k] = [str(p) for p in v]
        print(json.dumps(payload, indent=2, sort_keys=True, default=_asdict))
        return 0

    report_root = generate_report(args.spec)
    print(str(report_root))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

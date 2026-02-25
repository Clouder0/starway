from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _is_run_dir(p: Path) -> bool:
    return (p / "run_meta.json").is_file() and (p / "points").is_dir()


def _resolve_run_dir(path: Path) -> Path:
    path = Path(path).expanduser().resolve()
    if _is_run_dir(path):
        return path

    if not path.is_dir():
        raise SystemExit(f"Not a directory: {path}")

    candidates: list[Path] = []
    for child in path.iterdir():
        if not child.is_dir():
            continue
        if _is_run_dir(child):
            candidates.append(child)

    if not candidates:
        raise SystemExit(
            "Expected a run dir (run_meta.json + points/) or a campaign dir containing runs"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_num(x: Any) -> str:
    if isinstance(x, (int, float)):
        # Keep OCR-friendly: fixed-ish, not too many decimals.
        if abs(float(x)) >= 100:
            return f"{float(x):.1f}"
        return f"{float(x):.3f}"
    return "-"


def _find_point(
    points: list[dict[str, Any]],
    *,
    workload: str,
    comparator: str,
    want_params: dict[str, Any],
) -> dict[str, Any] | None:
    for p in points:
        if p.get("status") != "ok":
            continue
        if p.get("workload") != workload:
            continue
        if p.get("comparator") != comparator:
            continue

        params = p.get("params")
        if not isinstance(params, dict):
            continue

        ok = True
        for k, v in want_params.items():
            if params.get(k) != v:
                ok = False
                break
        if ok:
            return p
    return None


def _print_block(title: str) -> None:
    print("")
    print(title)
    print("-" * len(title))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print an OCR-friendly summary for a Starway bench run"
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Run dir (contains run_meta.json + points/) or campaign dir (contains runs)",
    )
    args = parser.parse_args(argv)

    run_dir = _resolve_run_dir(Path(args.path))
    run_meta = _load_json(run_dir / "run_meta.json")
    campaign_id = str(run_meta.get("campaign_id", run_dir.parent.name))
    run_id = str(run_meta.get("run_id", run_dir.name))
    config_hash = str(run_meta.get("config_hash", ""))

    points_dir = run_dir / "points"
    points: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    for p in sorted(points_dir.glob("*.json")):
        rec = _load_json(p)
        if isinstance(rec, dict):
            points.append(rec)
            status = str(rec.get("status", "<missing>"))
            status_counts[status] = status_counts.get(status, 0) + 1

    print("STARWAY_BENCH_OCR_SUMMARY")
    print(f"campaign_id: {campaign_id}")
    print(f"run_id:      {run_id}")
    if config_hash:
        print(f"config_hash: {config_hash[:8]}")
    print(f"run_dir:     {run_dir}")
    print(f"points_ok:   {status_counts.get('ok', 0)}")
    print(f"points_fail: {sum(v for k, v in status_counts.items() if k != 'ok')}")

    # Pull comparator env from run_meta for provenance.
    cfg = run_meta.get("config", {})
    comps = []
    if isinstance(cfg, dict):
        comps = cfg.get("comparators", [])
    if isinstance(comps, list) and comps:
        _print_block("comparators")
        for c in comps:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name", ""))
            typ = str(c.get("type", ""))
            env = c.get("env", {})
            env_txt = ""
            if isinstance(env, dict) and env:
                keys = sorted(str(k) for k in env.keys())
                parts = [f"{k}={env.get(k)}" for k in keys]
                env_txt = " " + " ".join(parts)
            if name:
                print(f"- {name}: {typ}{env_txt}")

    # Key point selectors. Keep it compact and stable for OCR.
    key_comparators = [
        "asyncio_tcp",
        "ucx_tcp_default",
        "ucx_tcp_tuned",
        "ucx_tcp",
    ]

    _print_block("bulk_stream (goodput_gbps)")
    bulk_params = {
        "chunk_bytes": "16MiB",
        "inflight": 4,
        "ack_every_n": 4,
        "duration_s": 2.0,
    }
    for comp in key_comparators:
        pt = _find_point(
            points, workload="bulk_stream", comparator=comp, want_params=bulk_params
        )
        if pt is None:
            continue
        gbps = None
        metrics = pt.get("metrics")
        if isinstance(metrics, dict):
            gbps = metrics.get("goodput_gbps")
        print(f"{comp:14s} {_fmt_num(gbps)}")

    _print_block("small_message_pressure@64KiB (goodput_gbps)")
    smp_params = {
        "msg_bytes": 65536,
        "inflight": 64,
        "ack_every_n": 64,
        "duration_s": 2.0,
    }
    for comp in key_comparators:
        pt = _find_point(
            points,
            workload="small_message_pressure",
            comparator=comp,
            want_params=smp_params,
        )
        if pt is None:
            continue
        gbps = None
        metrics = pt.get("metrics")
        if isinstance(metrics, dict):
            gbps = metrics.get("goodput_gbps")
        print(f"{comp:14s} {_fmt_num(gbps)}")

    _print_block("control_rtt (rtt_p99_us)")
    for msg_bytes in (64, 1048576):
        want = {"msg_bytes": msg_bytes, "iters": 2000, "warmup_iters": 200}
        row = []
        for comp in key_comparators:
            pt = _find_point(
                points, workload="control_rtt", comparator=comp, want_params=want
            )
            if pt is None:
                continue
            p99 = None
            metrics = pt.get("metrics")
            if isinstance(metrics, dict):
                p99 = metrics.get("rtt_p99_us")
            row.append((comp, p99))

        if row:
            print(f"msg_bytes={msg_bytes}")
            for comp, p99 in row:
                print(f"{comp:14s} {_fmt_num(p99)}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

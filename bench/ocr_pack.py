from __future__ import annotations

import argparse
import json
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bench.util import parse_size


def _is_run_dir(p: Path) -> bool:
    return (p / "run_meta.json").is_file() and (p / "points").is_dir()


def _resolve_run_dir(path: Path) -> Path:
    """Accept either a run dir or a campaign dir (pick newest run)."""

    path = Path(path).expanduser().resolve()
    if _is_run_dir(path):
        return path

    if not path.is_dir():
        raise SystemExit(f"Not a directory: {path}")

    candidates: list[Path] = []
    for child in path.iterdir():
        if child.is_dir() and _is_run_dir(child):
            candidates.append(child)

    if not candidates:
        raise SystemExit(
            "Expected a run dir (run_meta.json + points/) or a campaign dir containing runs"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt(x: Any) -> str:
    """OCR-friendly numeric formatting."""

    if x is None:
        return "NA"
    if isinstance(x, bool):
        return "1" if x else "0"
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        # Keep stable but compact; enough precision for performance plots.
        if abs(x) >= 1000:
            return f"{x:.1f}"
        if abs(x) >= 100:
            return f"{x:.2f}"
        return f"{x:.3f}"
    return str(x)


def _crc32_hex(lines: list[str]) -> str:
    # CRC32 is short and good enough as an OCR integrity check.
    blob = ("\n".join(lines) + "\n").encode("utf-8", errors="strict")
    return f"{zlib.crc32(blob) & 0xFFFFFFFF:08X}"


def _max_size_in_mix(sizes: Any) -> str:
    if isinstance(sizes, list):
        return _fmt_size(max(parse_size(s) for s in sizes) if sizes else 0)
    if isinstance(sizes, str):
        # Sometimes sizes can land as a JSON string in flattened forms.
        s = sizes.strip()
        if s.startswith("["):
            loaded = json.loads(s)
            if not isinstance(loaded, list):
                raise ValueError("sizes JSON must decode to a list")
            return _max_size_in_mix(loaded)
        return _fmt_size(parse_size(s))
    return "NA"


def _fmt_size(n_bytes: int) -> str:
    # Prefer the canonical strings already used by configs (KiB/MiB/GiB).
    n = int(n_bytes)
    if n % (1024**3) == 0 and n >= 1024**3:
        return f"{n // (1024**3)}GiB"
    if n % (1024**2) == 0 and n >= 1024**2:
        return f"{n // (1024**2)}MiB"
    if n % 1024 == 0 and n >= 1024:
        return f"{n // 1024}KiB"
    return f"{n}B"


@dataclass(frozen=True)
class ComparatorCol:
    code: str
    name: str
    typ: str
    env: dict[str, str]


def _default_code_for(name: str) -> str:
    n = name.lower()
    if "asyncio" in n:
        return "A"
    if "rdma" in n:
        return "R"
    if "tuned" in n:
        return "T"
    if "default" in n:
        return "D"
    if n in ("ucx_tcp", "ucx") or "ucx_tcp" in n:
        return "U"
    return "C"


def _build_comparator_cols(run_meta: dict[str, Any]) -> list[ComparatorCol]:
    cfg = run_meta.get("config")
    if not isinstance(cfg, dict):
        return []
    comps = cfg.get("comparators")
    if not isinstance(comps, list):
        return []

    used: set[str] = set()
    cols: list[ComparatorCol] = []
    for item in comps:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", ""))
        if not name:
            continue
        typ = str(item.get("type", ""))
        env = item.get("env", {})
        env = env if isinstance(env, dict) else {}
        env = {str(k): str(v) for k, v in env.items()}

        base = _default_code_for(name)
        code = base
        if code in used:
            # Disambiguate deterministically.
            i = 2
            while f"{base}{i}" in used:
                i += 1
            code = f"{base}{i}"
        used.add(code)

        cols.append(ComparatorCol(code=code, name=name, typ=typ, env=env))
    return cols


def _load_points(run_dir: Path) -> list[dict[str, Any]]:
    points_dir = run_dir / "points"
    out: list[dict[str, Any]] = []
    for p in sorted(points_dir.glob("*.json")):
        rec = _load_json(p)
        if isinstance(rec, dict):
            out.append(rec)
    return out


def _idx_points(
    points: list[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Index points by (workload, comparator, point_key)."""

    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for p in points:
        if p.get("status") != "ok":
            continue
        workload = str(p.get("workload", ""))
        comparator = str(p.get("comparator", ""))
        point_id = str(p.get("point_id", ""))
        if not workload or not comparator or not point_id:
            continue
        out[(workload, comparator, point_id)] = p
    return out


def _points_by_workload(
    points: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for p in points:
        if p.get("status") != "ok":
            continue
        w = str(p.get("workload", ""))
        if not w:
            continue
        out.setdefault(w, []).append(p)
    return out


def _emit_block(name: str, lines: list[str]) -> None:
    h = _crc32_hex(lines)
    print(f"BEGIN {name} H={h}")
    for ln in lines:
        print(ln)
    print(f"END {name} H={h}")


def _want_col_order(
    cols: list[ComparatorCol], *, prefer: list[str]
) -> list[ComparatorCol]:
    # Stable, human-friendly ordering: prefer known codes first, then the rest.
    by_code = {c.code: c for c in cols}
    out: list[ComparatorCol] = []
    used: set[str] = set()
    for code in prefer:
        if code in by_code:
            out.append(by_code[code])
            used.add(code)
    for c in cols:
        if c.code in used:
            continue
        out.append(c)
    return out


def _dense_table_control_rtt(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    # key: msg_bytes
    # val: metric.rtt_p99_us
    rows: dict[int, dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "control_rtt":
            continue
        params = p.get("params")
        metrics = p.get("metrics")
        if not isinstance(params, dict) or not isinstance(metrics, dict):
            continue
        msg_bytes = params.get("msg_bytes")
        if not isinstance(msg_bytes, int):
            continue
        p99 = metrics.get("rtt_p99_us")
        if not isinstance(p99, (int, float)):
            continue
        rows.setdefault(int(msg_bytes), {})[str(p.get("comparator"))] = float(p99)

    out: list[str] = []
    header = ["MSG_B"] + [c.code for c in cols]
    out.append(" ".join(f"{h:>10s}" for h in header))
    for msg_bytes in sorted(rows.keys()):
        line = [str(msg_bytes)]
        for c in cols:
            line.append(_fmt(rows[msg_bytes].get(c.name)))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def _dense_table_small_message_pressure(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    rows: dict[int, dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "small_message_pressure":
            continue
        params = p.get("params")
        metrics = p.get("metrics")
        if not isinstance(params, dict) or not isinstance(metrics, dict):
            continue
        msg_bytes = params.get("msg_bytes")
        if not isinstance(msg_bytes, int):
            continue
        gbps = metrics.get("goodput_gbps")
        if not isinstance(gbps, (int, float)):
            continue
        rows.setdefault(int(msg_bytes), {})[str(p.get("comparator"))] = float(gbps)

    out: list[str] = []
    header = ["MSG_B"] + [c.code for c in cols]
    out.append(" ".join(f"{h:>10s}" for h in header))
    for msg_bytes in sorted(rows.keys()):
        line = [str(msg_bytes)]
        for c in cols:
            line.append(_fmt(rows[msg_bytes].get(c.name)))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def _dense_table_bulk_stream(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    # key: (inflight, chunk_bytes_str, ack_every_n)
    # val: metric.goodput_gbps
    rows: dict[tuple[int, str, int], dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "bulk_stream":
            continue
        params = p.get("params")
        metrics = p.get("metrics")
        if not isinstance(params, dict) or not isinstance(metrics, dict):
            continue
        inflight = params.get("inflight")
        ack = params.get("ack_every_n")
        chunk = params.get("chunk_bytes")
        if not isinstance(inflight, int) or not isinstance(ack, int):
            continue
        if not isinstance(chunk, str):
            continue
        gbps = metrics.get("goodput_gbps")
        if not isinstance(gbps, (int, float)):
            continue
        key = (int(inflight), str(chunk), int(ack))
        rows.setdefault(key, {})[str(p.get("comparator"))] = float(gbps)

    def sort_key(k: tuple[int, str, int]) -> tuple[int, int, int]:
        inflight, chunk, ack = k
        return (int(inflight), int(parse_size(chunk)), int(ack))

    out: list[str] = []
    header = ["INF", "CHUNK", "ACK"] + [c.code for c in cols]
    out.append(" ".join(f"{h:>10s}" for h in header))
    for key in sorted(rows.keys(), key=sort_key):
        inflight, chunk, ack = key
        line = [str(inflight), str(chunk), str(ack)]
        for c in cols:
            line.append(_fmt(rows[key].get(c.name)))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def _dense_table_mixed_size(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    rows: dict[str, dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "mixed_size":
            continue
        params = p.get("params")
        metrics = p.get("metrics")
        if not isinstance(params, dict) or not isinstance(metrics, dict):
            continue
        max_size = _max_size_in_mix(params.get("sizes"))
        if max_size == "NA":
            continue
        gbps = metrics.get("goodput_gbps")
        if not isinstance(gbps, (int, float)):
            continue
        rows.setdefault(str(max_size), {})[str(p.get("comparator"))] = float(gbps)

    def sort_key(s: str) -> int:
        return int(parse_size(s))

    out: list[str] = []
    header = ["MAX"] + [c.code for c in cols]
    out.append(" ".join(f"{h:>10s}" for h in header))
    for max_size in sorted(rows.keys(), key=sort_key):
        line = [max_size]
        for c in cols:
            line.append(_fmt(rows[max_size].get(c.name)))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def _dense_table_bucket(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    rows: dict[str, dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "bucket_duplex_step":
            continue
        params = p.get("params")
        metrics = p.get("metrics")
        if not isinstance(params, dict) or not isinstance(metrics, dict):
            continue
        bucket = params.get("bucket_bytes")
        if not isinstance(bucket, str):
            continue
        agg = metrics.get("aggregate_gbps")
        if not isinstance(agg, (int, float)):
            continue
        rows.setdefault(str(bucket), {})[str(p.get("comparator"))] = float(agg)

    def sort_key(s: str) -> int:
        return int(parse_size(s))

    out: list[str] = []
    header = ["BUCKET"] + [c.code for c in cols]
    out.append(" ".join(f"{h:>10s}" for h in header))
    for bucket in sorted(rows.keys(), key=sort_key):
        line = [bucket]
        for c in cols:
            line.append(_fmt(rows[bucket].get(c.name)))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def _dense_table_control_under_load(
    cols: list[ComparatorCol], pts: list[dict[str, Any]]
) -> list[str]:
    # single point per comparator in the default matrix
    rows: dict[str, dict[str, float]] = {}
    for p in pts:
        if p.get("workload") != "control_under_load":
            continue
        metrics = p.get("metrics")
        if not isinstance(metrics, dict):
            continue
        bulk = metrics.get("bulk_goodput_gbps")
        ratio = metrics.get("p99_inflation_ratio")
        if not isinstance(bulk, (int, float)) or not isinstance(ratio, (int, float)):
            continue
        rows.setdefault("POINT", {})[f"{p.get('comparator')}.bulk"] = float(bulk)
        rows.setdefault("POINT", {})[f"{p.get('comparator')}.ratio"] = float(ratio)

    out: list[str] = []
    header = ["KIND"]
    for c in cols:
        header.extend([f"{c.code}_BULK", f"{c.code}_RATIO"])
    out.append(" ".join(f"{h:>10s}" for h in header))
    if "POINT" in rows:
        line = ["POINT"]
        for c in cols:
            line.append(_fmt(rows["POINT"].get(f"{c.name}.bulk")))
            line.append(_fmt(rows["POINT"].get(f"{c.name}.ratio")))
        out.append(" ".join(f"{x:>10s}" for x in line))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print a dense, OCR-friendly pack of all figure-relevant metrics"
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Run dir (contains run_meta.json + points/) or campaign dir (contains runs)",
    )
    args = parser.parse_args(argv)

    run_dir = _resolve_run_dir(Path(args.path))
    run_meta = _load_json(run_dir / "run_meta.json")
    if not isinstance(run_meta, dict):
        raise SystemExit("run_meta.json must be a JSON object")

    campaign_id = str(run_meta.get("campaign_id", run_dir.parent.name))
    run_id = str(run_meta.get("run_id", run_dir.name))
    config_hash = str(run_meta.get("config_hash", ""))
    platform = str(run_meta.get("platform", ""))
    pyver = str(run_meta.get("python", ""))

    points = _load_points(run_dir)
    status_counts: dict[str, int] = {}
    for p in points:
        status_counts[str(p.get("status", "<missing>"))] = (
            status_counts.get(str(p.get("status", "<missing>")), 0) + 1
        )

    cols = _build_comparator_cols(run_meta)
    cols = _want_col_order(cols, prefer=["A", "D", "T", "U", "R"])

    print("STARWAY_OCR_PACK_V1")
    print(f"CAMPAIGN {campaign_id}")
    print(f"RUN_ID   {run_id}")
    if config_hash:
        print(f"CFG_HASH {config_hash[:8]}")
    print(f"RUN_DIR  {run_dir}")
    if platform:
        print(f"PLATFORM {platform}")
    if pyver:
        print(f"PYTHON   {pyver.splitlines()[0]}")
    print(
        f"POINTS_OK {status_counts.get('ok', 0)} POINTS_FAIL {sum(v for k, v in status_counts.items() if k != 'ok')}"
    )

    if cols:
        print("COMPARATORS")
        for c in cols:
            env_keys = sorted(c.env.keys())
            env_txt = " ".join(f"{k}={c.env.get(k, '')}" for k in env_keys)
            if env_txt:
                print(f"  {c.code} {c.name} {c.typ} {env_txt}")
            else:
                print(f"  {c.code} {c.name} {c.typ}")

    by_w = _points_by_workload(points)

    # Emit only the metrics needed to reproduce the standard report figures.
    if "control_rtt" in by_w:
        _emit_block("CRTT_P99_US", _dense_table_control_rtt(cols, by_w["control_rtt"]))
    if "small_message_pressure" in by_w:
        _emit_block(
            "SMP_GBPS",
            _dense_table_small_message_pressure(cols, by_w["small_message_pressure"]),
        )
    if "bulk_stream" in by_w:
        _emit_block("BULK_GBPS", _dense_table_bulk_stream(cols, by_w["bulk_stream"]))
    if "mixed_size" in by_w:
        _emit_block("MIX_GBPS", _dense_table_mixed_size(cols, by_w["mixed_size"]))
    if "bucket_duplex_step" in by_w:
        _emit_block(
            "BUCKET_AGG_GBPS", _dense_table_bucket(cols, by_w["bucket_duplex_step"])
        )
    if "control_under_load" in by_w:
        _emit_block(
            "CONTROL_UNDER_LOAD",
            _dense_table_control_under_load(cols, by_w["control_under_load"]),
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

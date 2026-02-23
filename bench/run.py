from __future__ import annotations

import asyncio
import argparse
import platform
import sys
import time
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):  # Allow running as `python bench/run.py`
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

from bench.config import load_config, stable_config_hash  # noqa: E402
from bench.comparators import Pair  # noqa: E402
from bench.planner import plan_manifest  # noqa: E402
from bench.results import ResultWriter  # noqa: E402
from bench.workloads import WORKLOAD_RUNNERS, control_under_load  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Starway benchmark runner (MVP)")
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to campaign TOML config.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print expanded manifest and exit.",
    )
    return parser


async def _create_pair(
    comparator_type: str, *, env: dict[str, str], options: dict[str, Any]
) -> Pair:
    if comparator_type == "asyncio_tcp":
        from bench.comparators.asyncio_tcp import create_self_pair

        a, b = await create_self_pair(options)
        return Pair(a=a, b=b)

    if comparator_type == "starway_tcp":
        from bench.comparators.starway_tcp import create_self_pair

        a, b = await create_self_pair(env=env, options=options)
        return Pair(a=a, b=b)

    raise ValueError(f"Unknown comparator type '{comparator_type}'")


async def _close_pair(pair: Pair, *, close_timeout_s: float) -> None:
    # Best-effort: flush (when available) and close both sides.
    #
    # Starway/UCX can emit noisy warnings if one side closes while the other is
    # still flushing in-flight requests. We attempt a cooperative flush first.
    a = pair.a
    b = pair.b
    if hasattr(a, "client") and hasattr(b, "server") and hasattr(b, "endpoint"):
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    a.client.aflush(),  # type: ignore[attr-defined]
                    b.server.aflush_ep(b.endpoint),  # type: ignore[attr-defined]
                ),
                timeout=close_timeout_s,
            )
        except Exception:
            pass

    try:
        await asyncio.wait_for(a.close(), timeout=close_timeout_s)
    except Exception:
        pass
    try:
        await asyncio.wait_for(b.close(), timeout=close_timeout_s)
    except Exception:
        pass


async def run_campaign(config_path: Path, *, dry_run: bool) -> int:
    config = load_config(config_path)
    if config.execution.mode != "self":
        raise NotImplementedError(
            "cluster mode is TODO in MVP (config.execution.mode != 'self')"
        )

    manifest = plan_manifest(config)
    if dry_run:
        for point in manifest:
            print(point)
        print(f"Total points: {len(manifest)}")
        return 0

    config_hash = stable_config_hash(config)
    run_id = f"{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{config_hash[:8]}"
    out_dir = config.campaign.results_dir / config.campaign.id / run_id
    writer = ResultWriter(out_dir)

    run_meta = {
        "campaign_id": config.campaign.id,
        "run_id": run_id,
        "started_ts": time.time(),
        "platform": platform.platform(),
        "python": sys.version,
        "config_hash": config_hash,
        "config": asdict(config),
    }
    writer.write_run_meta(run_meta)
    writer.write_manifest(manifest)
    writer.log_event(
        {"type": "run_start", "ts": time.time(), "campaign_id": config.campaign.id}
    )

    comparator_by_name = {c.name: c for c in config.comparators}

    for point in manifest:
        writer.log_event(
            {
                "type": "point_start",
                "ts": time.time(),
                "point_id": point.point_id,
                "comparator": point.comparator,
                "workload": point.workload,
                "repeat": point.repeat,
            }
        )

        comparator = comparator_by_name[point.comparator]
        params = dict(point.params)
        params.setdefault("seed", int(config.campaign.seed) ^ int(point.point_id, 16))
        t0 = time.time()

        record: dict[str, Any] = {
            "campaign_id": config.campaign.id,
            "point_id": point.point_id,
            "comparator": point.comparator,
            "workload": point.workload,
            "repeat": point.repeat,
            "params": params,
            "status": "failed",
            "metrics": {},
            "started_ts": t0,
        }

        pair: Pair | None = None
        data_pair: Pair | None = None
        control_pair: Pair | None = None
        point_timeout_s = float(config.execution.point_timeout_s)
        close_timeout_s = float(config.execution.close_timeout_s)

        try:
            async with asyncio.timeout(point_timeout_s):
                if point.workload == "control_under_load":
                    data_pair = await _create_pair(
                        comparator.type, env=comparator.env, options=comparator.options
                    )
                    control_pair = await _create_pair(
                        comparator.type, env=comparator.env, options=comparator.options
                    )
                    result = await control_under_load(
                        data_pair=data_pair,
                        control_pair=control_pair,
                        params=params,
                    )
                else:
                    runner = WORKLOAD_RUNNERS.get(point.workload)
                    if runner is None:
                        raise ValueError(f"Unknown workload '{point.workload}'")
                    pair = await _create_pair(
                        comparator.type, env=comparator.env, options=comparator.options
                    )
                    result = await runner(pair, params)

            record["status"] = "ok"
            record["metrics"] = result.get("metrics", {})
            if "warnings" in result:
                record["warnings"] = result["warnings"]
            for k in ("subresults",):
                if k in result:
                    record[k] = result[k]
        except TimeoutError as exc:
            record["status"] = "failed"
            record["error"] = (
                str(exc) or f"Point timed out after {point_timeout_s:.3f}s"
            )
            record["traceback"] = traceback.format_exc()
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = str(exc)
            record["traceback"] = traceback.format_exc()
        finally:
            if pair is not None:
                await _close_pair(pair, close_timeout_s=close_timeout_s)
            if data_pair is not None:
                await _close_pair(data_pair, close_timeout_s=close_timeout_s)
            if control_pair is not None:
                await _close_pair(control_pair, close_timeout_s=close_timeout_s)
            record["ended_ts"] = time.time()
            record["elapsed_s"] = float(record["ended_ts"] - record["started_ts"])
            writer.write_point(point.point_id, record)
            writer.log_event(
                {
                    "type": "point_end",
                    "ts": time.time(),
                    "point_id": point.point_id,
                    "status": record["status"],
                }
            )

    writer.log_event(
        {"type": "run_end", "ts": time.time(), "campaign_id": config.campaign.id}
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(run_campaign(args.config, dry_run=bool(args.dry_run)))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

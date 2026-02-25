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
from bench.planner import plan_manifest  # noqa: E402
from bench.results import ResultWriter  # noqa: E402


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


async def run_campaign(config_path: Path, *, dry_run: bool) -> int:
    config = load_config(config_path)

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

    if config.execution.mode == "self":
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
            params.setdefault(
                "seed", int(config.campaign.seed) ^ int(point.point_id, 16)
            )
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

            point_timeout_s = float(config.execution.point_timeout_s)

            try:
                async with asyncio.timeout(point_timeout_s):
                    from bench.topology_2proc import run_two_process_point

                    result = await run_two_process_point(
                        comparator_type=comparator.type,
                        comparator_env=comparator.env,
                        comparator_options=comparator.options,
                        workload=point.workload,
                        params=params,
                        cpu_affinity=tuple(config.execution.cpu_affinity),
                        payload_verify=config.execution.payload_verify,
                    )

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
    elif config.execution.mode == "cluster":
        if config.cluster is None:
            raise ValueError("cluster config is required when execution.mode='cluster'")

        from bench.topology_cluster import ClusterSession

        async with ClusterSession(cluster=config.cluster) as session:
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
                params.setdefault(
                    "seed", int(config.campaign.seed) ^ int(point.point_id, 16)
                )
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

                point_timeout_s = float(config.execution.point_timeout_s)
                point_spec = {
                    "comparator_type": comparator.type,
                    "comparator_env": dict(comparator.env),
                    "comparator_options": dict(comparator.options),
                    "workload": str(point.workload),
                    "params": params,
                    "cpu_affinity": list(config.execution.cpu_affinity),
                    "payload_verify": config.execution.payload_verify,
                    "point_timeout_s": point_timeout_s,
                }

                try:
                    async with asyncio.timeout(point_timeout_s):
                        connect, result = await session.run_point(
                            point_id=point.point_id,
                            point_spec=point_spec,
                        )

                    record["status"] = "ok"
                    record["connect"] = connect
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
                    try:
                        await session.cancel_point(point_id=point.point_id)
                    except Exception:
                        pass
                except Exception as exc:
                    record["status"] = "failed"
                    record["error"] = str(exc)
                    record["traceback"] = traceback.format_exc()
                    try:
                        await session.cancel_point(point_id=point.point_id)
                    except Exception:
                        pass
                finally:
                    record["ended_ts"] = time.time()
                    record["elapsed_s"] = float(
                        record["ended_ts"] - record["started_ts"]
                    )
                    writer.write_point(point.point_id, record)
                    writer.log_event(
                        {
                            "type": "point_end",
                            "ts": time.time(),
                            "point_id": point.point_id,
                            "status": record["status"],
                        }
                    )
    else:
        raise ValueError(f"unknown execution.mode: {config.execution.mode}")

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

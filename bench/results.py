from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable

from .types import ManifestPoint


class ResultWriter:
    """Write run artifacts in a predictable layout."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.points_dir = base_dir / "points"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.points_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, rel_path: str, payload: Any) -> None:
        path = self.base_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

    def append_jsonl(self, rel_path: str, payload: Any) -> None:
        path = self.base_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True, default=str))
            f.write("\n")

    def write_run_meta(self, payload: dict[str, Any]) -> None:
        self.write_json("run_meta.json", payload)

    def write_manifest(self, manifest: Iterable[ManifestPoint]) -> None:
        self.write_json("manifest.json", [asdict(p) for p in manifest])

    def write_point(self, point_id: str, payload: dict[str, Any]) -> None:
        self.write_json(f"points/{point_id}.json", payload)

    def log_event(self, payload: dict[str, Any]) -> None:
        self.append_jsonl("events.jsonl", payload)

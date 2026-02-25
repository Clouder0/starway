from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import math

import matplotlib

# Force a non-interactive backend (safe for CI/headless machines).
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from .util import parse_size

_PALETTE = [
    "#1f77b4",  # matplotlib default blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
]

_LINESTYLE_FOR_CONDITION = {"baseline": "--", "new": "-"}
_MARKER_FOR_CONDITION = {"baseline": "x", "new": "o"}


def _color_for_comparators(comparators: list[str]) -> dict[str, str]:
    return {c: _PALETTE[i % len(_PALETTE)] for i, c in enumerate(comparators)}


@dataclass(frozen=True)
class FigureArtifact:
    figure_id: str
    pdf_path: Path
    caption: str
    label: str
    alt: str


def render_figure(
    figure_id: str,
    rows: list[dict[str, Any]],
    out_dir: Path,
) -> FigureArtifact:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if figure_id == "control_rtt_p99":
        return _render_control_rtt_p99(rows, out_dir)

    if figure_id == "bulk_stream_goodput":
        return _render_bulk_stream_goodput(rows, out_dir)

    if figure_id == "small_message_pressure_goodput":
        return _render_small_message_pressure_goodput(rows, out_dir)

    if figure_id == "mixed_size_goodput":
        return _render_mixed_size_goodput(rows, out_dir)

    if figure_id == "bucket_duplex_step_aggregate":
        return _render_bucket_duplex_step_aggregate(rows, out_dir)

    if figure_id == "control_under_load_inflation":
        return _render_control_under_load_inflation(rows, out_dir)

    raise ValueError(f"Unknown figure_id: {figure_id}")


def _bytes_from_param(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return float(parse_size(v))
    raise TypeError(f"unsupported size value: {type(v).__name__}")


def _max_size_from_sizes_param(v: Any) -> float:
    """Return the max element size for mixed-size workload params."""

    sizes: list[Any]
    if isinstance(v, list):
        sizes = v
    elif isinstance(v, tuple):
        sizes = list(v)
    elif isinstance(v, str):
        s = v.strip()
        if s.startswith("["):
            import json

            loaded = json.loads(s)
            if not isinstance(loaded, list):
                raise ValueError("param.sizes must decode to a list")
            sizes = loaded
        else:
            sizes = [s]
    else:
        raise TypeError(f"unsupported sizes param type: {type(v).__name__}")

    if not sizes:
        return 0.0
    return float(max(_bytes_from_param(x) for x in sizes))


def format_bytes_binary(value: float | int) -> str:
    """Format a byte count using IEC binary units (KiB/MiB/GiB).

    Matplotlib tick formatters typically pass floats; this helper snaps values
    that are effectively integers.
    """

    x = float(value)
    if not math.isfinite(x):
        return ""
    sign = "-" if x < 0 else ""
    x = abs(x)

    # Snap close-to-integers (common for powers-of-two tick positions).
    x_int: int | None = None
    x_round = round(x)
    if abs(x - x_round) < 1e-6:
        x_int = int(x_round)
        x = float(x_int)

    if x < 1024:
        return f"{sign}{int(x)}B"

    units = [
        ("TiB", 1024**4),
        ("GiB", 1024**3),
        ("MiB", 1024**2),
        ("KiB", 1024),
    ]
    for unit, mult in units:
        if x >= mult:
            if x_int is not None and x_int % mult == 0:
                return f"{sign}{x_int // mult}{unit}"
            val = x / mult
            # Keep labels compact (avoid long decimals).
            return f"{sign}{val:g}{unit}"

    # Fallback (should not happen due to x < 1024 early-return).
    return f"{sign}{int(x)}B"


def _render_control_rtt_p99(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    # Filter to the relevant workload.
    data = [
        r
        for r in rows
        if r.get("workload") == "control_rtt"
        and isinstance(r.get("param.msg_bytes"), (int, float))
        and isinstance(r.get("metric.rtt_p99_us"), (int, float))
    ]
    if not data:
        raise ValueError("No rows for control_rtt with metric.rtt_p99_us")

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    # Stable ordering and styling.
    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    for comp in comparators:
        for condition in ("baseline", "new"):
            pts = [
                (float(r["param.msg_bytes"]), float(r["metric.rtt_p99_us"]))
                for r in data
                if str(r.get("comparator")) == comp and r.get("condition") == condition
            ]
            if not pts:
                continue
            pts.sort(key=lambda xy: xy[0])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(
                xs,
                ys,
                label=(comp if single_condition else f"{comp} ({condition})"),
                color=color_for[comp],
                linestyle=_LINESTYLE_FOR_CONDITION.get(condition, "-"),
                marker=_MARKER_FOR_CONDITION.get(condition, "o"),
                markersize=3.5,
                linewidth=1.6,
            )

    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _pos: format_bytes_binary(x))
    )
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_xlabel("Message size")
    ax.set_ylabel("RTT p99 (us)")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    ax.legend(loc="best", frameon=False, fontsize=8)

    pdf_path = out_dir / "fig-control_rtt_p99.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="control_rtt_p99",
        pdf_path=pdf_path,
        caption="Control RTT (p99) versus message size.",
        label="fig:control-rtt-p99",
        alt="Control RTT p99 versus message size.",
    )


def _render_bulk_stream_goodput(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    data = [
        r
        for r in rows
        if r.get("workload") == "bulk_stream"
        and "param.chunk_bytes" in r
        and isinstance(r.get("param.inflight"), (int, float))
        and isinstance(r.get("metric.goodput_gbps"), (int, float))
    ]
    if not data:
        raise ValueError("No rows for bulk_stream with metric.goodput_gbps")

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    inflights = sorted({int(r["param.inflight"]) for r in data})
    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    n = len(inflights)
    ncols = 2 if n > 1 else 1
    nrows = (n + ncols - 1) // ncols
    fig, axs = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(6.0 * ncols, 3.2 * nrows),
        squeeze=False,
        sharex=True,
        sharey=True,
    )

    for idx, inflight in enumerate(inflights):
        ax = axs[idx // ncols][idx % ncols]
        for comp in comparators:
            for condition in ("baseline", "new"):
                pts = [
                    (
                        _bytes_from_param(r["param.chunk_bytes"]),
                        float(r["metric.goodput_gbps"]),
                    )
                    for r in data
                    if int(r["param.inflight"]) == inflight
                    and str(r.get("comparator")) == comp
                    and r.get("condition") == condition
                ]
                if not pts:
                    continue
                pts.sort(key=lambda xy: xy[0])
                xs = [p[0] for p in pts]
                ys = [p[1] for p in pts]
                ax.plot(
                    xs,
                    ys,
                    label=(comp if single_condition else f"{comp} ({condition})"),
                    color=color_for[comp],
                    linestyle=_LINESTYLE_FOR_CONDITION.get(condition, "-"),
                    marker=_MARKER_FOR_CONDITION.get(condition, "o"),
                    markersize=3.5,
                    linewidth=1.6,
                )

        ax.set_title(f"inflight={inflight}")
        ax.set_xscale("log", base=2)
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _pos: format_bytes_binary(x))
        )
        ax.xaxis.set_minor_formatter(mticker.NullFormatter())
        ax.grid(True, which="both", axis="both", alpha=0.25)

    # Hide unused axes.
    for j in range(n, nrows * ncols):
        axs[j // ncols][j % ncols].axis("off")

    for ax in axs[-1]:
        ax.set_xlabel("Chunk size")
    for i in range(nrows):
        axs[i][0].set_ylabel("Goodput (Gbps)")

    # Single, de-duplicated legend across subplots.
    handles: list[Any] = []
    labels: list[str] = []
    seen: set[str] = set()
    for row in axs:
        for ax in row:
            hs, ls = ax.get_legend_handles_labels()
            for h, l in zip(hs, ls, strict=False):
                if l and l not in seen:
                    seen.add(l)
                    handles.append(h)
                    labels.append(l)

    if handles:
        ncol = min(4, max(1, len(labels)))
        nrows = (len(labels) + ncol - 1) // ncol
        fig.legend(
            handles,
            labels,
            loc="lower center",
            ncol=ncol,
            frameon=False,
            fontsize=8,
        )
        fig.subplots_adjust(bottom=0.16 + 0.05 * max(0, nrows - 1))

    pdf_path = out_dir / "fig-bulk_stream_goodput.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="bulk_stream_goodput",
        pdf_path=pdf_path,
        caption="Bulk stream goodput versus chunk size.",
        label="fig:bulk-stream-goodput",
        alt="Bulk stream goodput versus chunk size.",
    )


def _render_small_message_pressure_goodput(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    data = [
        r
        for r in rows
        if r.get("workload") == "small_message_pressure"
        and isinstance(r.get("param.msg_bytes"), (int, float))
        and isinstance(r.get("metric.goodput_gbps"), (int, float))
    ]
    if not data:
        raise ValueError(
            "No rows for small_message_pressure with param.msg_bytes and metric.goodput_gbps"
        )

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    for comp in comparators:
        for condition in ("baseline", "new"):
            pts = [
                (float(r["param.msg_bytes"]), float(r["metric.goodput_gbps"]))
                for r in data
                if str(r.get("comparator")) == comp and r.get("condition") == condition
            ]
            if not pts:
                continue
            pts.sort(key=lambda xy: xy[0])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(
                xs,
                ys,
                label=(comp if single_condition else f"{comp} ({condition})"),
                color=color_for[comp],
                linestyle=_LINESTYLE_FOR_CONDITION.get(condition, "-"),
                marker=_MARKER_FOR_CONDITION.get(condition, "o"),
                markersize=3.5,
                linewidth=1.6,
            )

    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _pos: format_bytes_binary(x))
    )
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_xlabel("Message size")
    ax.set_ylabel("Goodput (Gbps)")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    ax.legend(loc="best", frameon=False, fontsize=8)

    pdf_path = out_dir / "fig-small_message_pressure_goodput.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="small_message_pressure_goodput",
        pdf_path=pdf_path,
        caption="Small-message pressure goodput versus message size.",
        label="fig:small-message-pressure-goodput",
        alt="Small-message pressure goodput versus message size.",
    )


def _render_mixed_size_goodput(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    data = [
        r
        for r in rows
        if r.get("workload") == "mixed_size"
        and "param.sizes" in r
        and isinstance(r.get("metric.goodput_gbps"), (int, float))
    ]
    if not data:
        raise ValueError(
            "No rows for mixed_size with param.sizes and metric.goodput_gbps"
        )

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    for comp in comparators:
        for condition in ("baseline", "new"):
            pts = [
                (
                    _max_size_from_sizes_param(r["param.sizes"]),
                    float(r["metric.goodput_gbps"]),
                )
                for r in data
                if str(r.get("comparator")) == comp and r.get("condition") == condition
            ]
            if not pts:
                continue
            pts.sort(key=lambda xy: xy[0])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(
                xs,
                ys,
                label=(comp if single_condition else f"{comp} ({condition})"),
                color=color_for[comp],
                linestyle=_LINESTYLE_FOR_CONDITION.get(condition, "-"),
                marker=_MARKER_FOR_CONDITION.get(condition, "o"),
                markersize=3.5,
                linewidth=1.6,
            )

    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _pos: format_bytes_binary(x))
    )
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_xlabel("Max size in mix")
    ax.set_ylabel("Goodput (Gbps)")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    ax.legend(loc="best", frameon=False, fontsize=8)

    pdf_path = out_dir / "fig-mixed_size_goodput.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="mixed_size_goodput",
        pdf_path=pdf_path,
        caption="Mixed-size goodput versus max size in the payload mix.",
        label="fig:mixed-size-goodput",
        alt="Mixed-size goodput versus max size in the payload mix.",
    )


def _render_bucket_duplex_step_aggregate(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    data = [
        r
        for r in rows
        if r.get("workload") == "bucket_duplex_step"
        and "param.bucket_bytes" in r
        and isinstance(r.get("metric.aggregate_gbps"), (int, float))
    ]
    if not data:
        raise ValueError("No rows for bucket_duplex_step with metric.aggregate_gbps")

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    for comp in comparators:
        for condition in ("baseline", "new"):
            pts = [
                (
                    _bytes_from_param(r["param.bucket_bytes"]),
                    float(r["metric.aggregate_gbps"]),
                )
                for r in data
                if str(r.get("comparator")) == comp and r.get("condition") == condition
            ]
            if not pts:
                continue
            pts.sort(key=lambda xy: xy[0])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(
                xs,
                ys,
                label=(comp if single_condition else f"{comp} ({condition})"),
                color=color_for[comp],
                linestyle=_LINESTYLE_FOR_CONDITION.get(condition, "-"),
                marker=_MARKER_FOR_CONDITION.get(condition, "o"),
                markersize=3.5,
                linewidth=1.6,
            )

    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _pos: format_bytes_binary(x))
    )
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_xlabel("Bucket size")
    ax.set_ylabel("Aggregate bandwidth (Gbps)")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    ax.legend(loc="best", frameon=False, fontsize=8)

    pdf_path = out_dir / "fig-bucket_duplex_step_aggregate.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="bucket_duplex_step_aggregate",
        pdf_path=pdf_path,
        caption="Bucket duplex aggregate bandwidth versus bucket size.",
        label="fig:bucket-duplex-aggregate",
        alt="Bucket duplex aggregate bandwidth versus bucket size.",
    )


def _render_control_under_load_inflation(
    rows: list[dict[str, Any]], out_dir: Path
) -> FigureArtifact:
    data = [
        r
        for r in rows
        if r.get("workload") == "control_under_load"
        and isinstance(r.get("metric.bulk_goodput_gbps"), (int, float))
        and isinstance(r.get("metric.p99_inflation_ratio"), (int, float))
    ]
    if not data:
        raise ValueError(
            "No rows for control_under_load with metric.bulk_goodput_gbps and metric.p99_inflation_ratio"
        )

    conditions_present = {
        str(r.get("condition"))
        for r in data
        if r.get("condition") in ("baseline", "new")
    }
    single_condition = len(conditions_present) == 1

    comparators = sorted({str(r.get("comparator", "")) for r in data})
    color_for = _color_for_comparators(comparators)

    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    for comp in comparators:
        for condition in ("baseline", "new"):
            pts = [
                (
                    float(r["metric.bulk_goodput_gbps"]),
                    float(r["metric.p99_inflation_ratio"]),
                )
                for r in data
                if str(r.get("comparator")) == comp and r.get("condition") == condition
            ]
            if not pts:
                continue
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.scatter(
                xs,
                ys,
                label=(comp if single_condition else f"{comp} ({condition})"),
                color=color_for[comp],
                marker=_MARKER_FOR_CONDITION.get(condition, "o"),
            )

    ax.set_xlabel("Bulk goodput (Gbps)")
    ax.set_ylabel("Control p99 inflation ratio")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    ax.legend(loc="best", frameon=False, fontsize=8)

    pdf_path = out_dir / "fig-control_under_load_inflation.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)

    return FigureArtifact(
        figure_id="control_under_load_inflation",
        pdf_path=pdf_path,
        caption="Control-plane p99 inflation ratio versus bulk goodput.",
        label="fig:control-under-load-inflation",
        alt="Control-plane p99 inflation ratio versus bulk goodput.",
    )

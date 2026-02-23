from __future__ import annotations

from typing import Any


def parse_size(value: Any) -> int:
    """Parse human-friendly byte sizes.

    Accepts ints (returned as-is) and strings like: "64B", "1KiB", "4MiB", "1G".
    """

    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        raise TypeError(f"size must be int/float/str, got {type(value).__name__}")

    text = value.strip().lower().replace("_", "")
    if not text:
        raise ValueError("empty size string")

    suffixes = {
        "gib": 1024**3,
        "gb": 1024**3,
        "gi": 1024**3,
        "g": 1024**3,
        "mib": 1024**2,
        "mb": 1024**2,
        "mi": 1024**2,
        "m": 1024**2,
        "kib": 1024,
        "kb": 1024,
        "ki": 1024,
        "k": 1024,
        "b": 1,
    }
    # Match longest suffix first (e.g., "kib" before "k").
    for suffix in sorted(suffixes, key=len, reverse=True):
        mult = suffixes[suffix]
        if text.endswith(suffix):
            number_text = text[: -len(suffix)]
            if not number_text:
                raise ValueError(f"invalid size: '{value}'")
            number = float(number_text)
            return int(number * mult)

    return int(float(text))

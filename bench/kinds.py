from __future__ import annotations

# Reserved message kinds used by the benchmark harness itself.
#
# Workloads typically use small integers (1..N). Keep harness kinds in a high
# range to avoid accidental collisions.

KIND_BARRIER = 250
KIND_PREFLIGHT = 251

__all__ = ["KIND_BARRIER", "KIND_PREFLIGHT"]

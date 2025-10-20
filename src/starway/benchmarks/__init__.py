from __future__ import annotations

from .scenarios import SCENARIOS, ScenarioDefinition, ScenarioResult

__all__ = [
    "SCENARIOS",
    "ScenarioDefinition",
    "ScenarioResult",
    "list_scenarios",
    "get_scenario",
]


def list_scenarios() -> list[str]:
    """Return available benchmark scenario names."""
    return list(SCENARIOS.keys())


def get_scenario(name: str) -> ScenarioDefinition:
    """Fetch a scenario definition by name."""
    try:
        return SCENARIOS[name]
    except KeyError as exc:  # pragma: no cover - guard against typos
        raise ValueError(f"Unknown benchmark scenario '{name}'") from exc

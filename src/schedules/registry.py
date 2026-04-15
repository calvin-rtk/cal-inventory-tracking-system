"""Maps league name strings to their schedule provider classes."""
from __future__ import annotations

from .mlb import MLBSchedule
from .nhl import NHLSchedule
from .nba import NBASchedule
from .nfl import NFLSchedule
from .base import BaseSchedule

_REGISTRY: dict[str, type[BaseSchedule]] = {
    "MLB": MLBSchedule,
    "NHL": NHLSchedule,
    "NBA": NBASchedule,
    "NFL": NFLSchedule,
}


def get_schedule_provider(league: str) -> BaseSchedule:
    league_upper = league.upper()
    if league_upper not in _REGISTRY:
        supported = ", ".join(_REGISTRY.keys())
        raise ValueError(
            f"Unsupported league '{league}'. Supported: {supported}"
        )
    return _REGISTRY[league_upper]()

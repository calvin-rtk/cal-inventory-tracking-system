"""Abstract base class for official schedule providers."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import requests

from ..models import ScheduledGame

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


class BaseSchedule(ABC):
    LEAGUE: str = ""

    def get_games(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        """Return all scheduled games in [from_date, to_date], inclusive."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Shared HTTP helper
    # ------------------------------------------------------------------

    def _get_json(self, url: str, params: dict | None = None, timeout: int = 30) -> dict | list:
        resp = requests.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

"""
MLB schedule via the official MLB Stats API (free, no auth required).

Docs: https://statsapi.mlb.com/docs/
"""
from __future__ import annotations

import logging
from datetime import date

from ..models import ScheduledGame
from .base import BaseSchedule

logger = logging.getLogger(__name__)

_API_BASE = "https://statsapi.mlb.com/api/v1"


class MLBSchedule(BaseSchedule):
    LEAGUE = "MLB"

    def get_games(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        logger.info("[MLB] Fetching schedule %s → %s", from_date, to_date)

        params = {
            "sportId": 1,
            "startDate": from_date.strftime("%Y-%m-%d"),
            "endDate": to_date.strftime("%Y-%m-%d"),
            "hydrate": "team,venue",
            "gameType": "R,F,D,L,W",  # Regular, WC, Div, LCS, WS
        }

        try:
            data = self._get_json(f"{_API_BASE}/schedule", params=params)
        except Exception as exc:
            logger.error("[MLB] API request failed: %s", exc)
            return []

        games: list[ScheduledGame] = []
        for day in data.get("dates", []):
            game_date_str = day.get("date", "")
            try:
                game_date = date.fromisoformat(game_date_str)
            except ValueError:
                continue

            for game in day.get("games", []):
                home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                venue = game.get("venue", {}).get("name", "")
                game_id = str(game.get("gamePk", ""))

                if home and away:
                    games.append(
                        ScheduledGame(
                            game_date=game_date,
                            home_team=home,
                            away_team=away,
                            league="MLB",
                            game_id=game_id,
                            venue=venue,
                        )
                    )

        logger.info("[MLB] Retrieved %d games.", len(games))
        return games

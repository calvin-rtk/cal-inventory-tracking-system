"""
NHL schedule via the NHL API v1 (free, no auth required).

The endpoint returns one week at a time; we paginate via nextStartDate.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from ..models import ScheduledGame
from .base import BaseSchedule

logger = logging.getLogger(__name__)

_API_BASE = "https://api-web.nhle.com/v1"
_MAX_WEEKS = 52  # Safety limit


class NHLSchedule(BaseSchedule):
    LEAGUE = "NHL"

    def get_games(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        logger.info("[NHL] Fetching schedule %s → %s", from_date, to_date)

        games: list[ScheduledGame] = []
        current = from_date
        weeks_fetched = 0

        while current <= to_date and weeks_fetched < _MAX_WEEKS:
            url = f"{_API_BASE}/schedule/{current.strftime('%Y-%m-%d')}"
            try:
                data = self._get_json(url)
            except Exception as exc:
                logger.error("[NHL] API request failed for %s: %s", current, exc)
                break

            for week in data.get("gameWeek", []):
                for game in week.get("games", []):
                    raw_date = game.get("gameDate") or game.get("startTimeUTC", "")
                    try:
                        game_date = date.fromisoformat(raw_date[:10])
                    except (ValueError, TypeError):
                        continue

                    if game_date < from_date or game_date > to_date:
                        continue

                    home_info = game.get("homeTeam", {})
                    away_info = game.get("awayTeam", {})

                    home = _team_name(home_info)
                    away = _team_name(away_info)
                    venue = game.get("venue", {}).get("default", "")
                    game_id = str(game.get("id", ""))

                    if home and away:
                        games.append(
                            ScheduledGame(
                                game_date=game_date,
                                home_team=home,
                                away_team=away,
                                league="NHL",
                                game_id=game_id,
                                venue=venue,
                            )
                        )

            # Advance to next week
            next_start = data.get("nextStartDate")
            if next_start:
                try:
                    current = date.fromisoformat(next_start)
                except ValueError:
                    break
            else:
                current += timedelta(weeks=1)

            weeks_fetched += 1

        logger.info("[NHL] Retrieved %d games.", len(games))
        return games


def _team_name(team: dict) -> str:
    """Build a readable team name from the NHL API response."""
    place = team.get("placeName", {}).get("default", "")
    common = team.get("commonName", {}).get("default", "")
    if place and common:
        return f"{place} {common}"
    return common or place or team.get("abbrev", "")

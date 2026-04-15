"""
NFL schedule via the ESPN API (free, no auth required).

We fetch by season and type to get the full schedule rather than
paging day-by-day.  Regular season = type 2; preseason = type 1;
postseason = type 3.
"""
from __future__ import annotations

import logging
from datetime import date

from ..models import ScheduledGame
from .base import BaseSchedule
from .nba import _espn_event_to_game  # shared ESPN event parser

logger = logging.getLogger(__name__)

_ESPN_EVENTS = (
    "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl"
    "/seasons/{year}/types/{season_type}/events"
)
_ESPN_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
)


class NFLSchedule(BaseSchedule):
    LEAGUE = "NFL"

    def get_games(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        logger.info("[NFL] Fetching schedule %s → %s", from_date, to_date)

        games: list[ScheduledGame] = []

        # Determine which seasons to query (may span two calendar years for
        # a season that starts in Sep and ends in Feb).
        years_to_check = {from_date.year, to_date.year}
        for year in sorted(years_to_check):
            for season_type in (1, 2, 3):  # pre, regular, post
                games.extend(self._fetch_season(year, season_type, from_date, to_date))

        # Deduplicate by game_id
        seen: set[str] = set()
        unique: list[ScheduledGame] = []
        for g in games:
            key = g.game_id or f"{g.game_date}|{g.home_team}|{g.away_team}"
            if key not in seen:
                seen.add(key)
                unique.append(g)

        logger.info("[NFL] Retrieved %d games.", len(unique))
        return unique

    # ------------------------------------------------------------------

    def _fetch_season(
        self, year: int, season_type: int, from_date: date, to_date: date
    ) -> list[ScheduledGame]:
        url = _ESPN_EVENTS.format(year=year, season_type=season_type)
        try:
            data = self._get_json(url, params={"limit": 1000})
        except Exception as exc:
            logger.debug("[NFL] Season %d type %d fetch error: %s", year, season_type, exc)
            return []

        games: list[ScheduledGame] = []

        # ESPN core API returns items as refs; if items are embedded, parse them
        for item in data.get("items", []):
            # Items may be inline or just $ref links
            if "$ref" in item and "date" not in item:
                try:
                    item = self._get_json(item["$ref"])
                except Exception:
                    continue

            game = _espn_event_to_game(item, "NFL")
            if game and from_date <= game.game_date <= to_date:
                games.append(game)

        # Fallback: if core API returned nothing, try scoreboard by week
        if not games:
            games = self._fetch_scoreboard_fallback(year, season_type, from_date, to_date)

        return games

    def _fetch_scoreboard_fallback(
        self, year: int, season_type: int, from_date: date, to_date: date
    ) -> list[ScheduledGame]:
        games: list[ScheduledGame] = []
        for week in range(1, 23):
            try:
                data = self._get_json(
                    _ESPN_SCOREBOARD,
                    params={
                        "limit": 100,
                        "seasontype": season_type,
                        "week": week,
                        "dates": str(year),
                    },
                )
            except Exception:
                break

            events = data.get("events", [])
            if not events:
                break

            for event in events:
                game = _espn_event_to_game(event, "NFL")
                if game and from_date <= game.game_date <= to_date:
                    games.append(game)

        return games

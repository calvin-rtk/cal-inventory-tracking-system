"""
NBA schedule via the NBA CDN static JSON (free, no auth required).

Primary:  https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json
Fallback: ESPN scoreboard API (date-by-date pagination)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from ..models import ScheduledGame
from .base import BaseSchedule

logger = logging.getLogger(__name__)

_NBA_CDN = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"


class NBASchedule(BaseSchedule):
    LEAGUE = "NBA"

    def get_games(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        logger.info("[NBA] Fetching schedule %s → %s", from_date, to_date)

        games = self._fetch_from_cdn(from_date, to_date)
        if not games:
            logger.warning("[NBA] CDN returned no games; falling back to ESPN API.")
            games = self._fetch_from_espn(from_date, to_date)

        logger.info("[NBA] Retrieved %d games.", len(games))
        return games

    # ------------------------------------------------------------------

    def _fetch_from_cdn(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        try:
            data = self._get_json(_NBA_CDN)
        except Exception as exc:
            logger.warning("[NBA] CDN request failed: %s", exc)
            return []

        games: list[ScheduledGame] = []
        game_dates = (
            data.get("leagueSchedule", {}).get("gameDates", [])
        )

        for day in game_dates:
            raw_date = day.get("gameDate", "")  # e.g. "04/13/2025 00:00:00"
            try:
                game_date = date.fromisoformat(raw_date[:10]) if "-" in raw_date[:10] \
                    else date(int(raw_date[6:10]), int(raw_date[:2]), int(raw_date[3:5]))
            except (ValueError, IndexError):
                continue

            if game_date < from_date or game_date > to_date:
                continue

            for game in day.get("games", []):
                home_info = game.get("homeTeam", {})
                away_info = game.get("awayTeam", {})
                home = f"{home_info.get('teamCity', '')} {home_info.get('teamName', '')}".strip()
                away = f"{away_info.get('teamCity', '')} {away_info.get('teamName', '')}".strip()

                if home and away:
                    games.append(
                        ScheduledGame(
                            game_date=game_date,
                            home_team=home,
                            away_team=away,
                            league="NBA",
                            game_id=game.get("gameId", ""),
                        )
                    )
        return games

    def _fetch_from_espn(self, from_date: date, to_date: date) -> list[ScheduledGame]:
        """Paginate ESPN's scoreboard one day at a time."""
        games: list[ScheduledGame] = []
        current = from_date
        while current <= to_date:
            try:
                data = self._get_json(
                    _ESPN_BASE,
                    params={"dates": current.strftime("%Y%m%d"), "limit": 100},
                )
                for event in data.get("events", []):
                    game = _espn_event_to_game(event, "NBA")
                    if game:
                        games.append(game)
            except Exception as exc:
                logger.debug("[NBA/ESPN] %s: %s", current, exc)
            current += timedelta(days=1)
        return games


def _espn_event_to_game(event: dict, league: str) -> ScheduledGame | None:
    try:
        raw_date = event["date"][:10]
        game_date = date.fromisoformat(raw_date)
        competitors = event["competitions"][0]["competitors"]
        home = next(c for c in competitors if c["homeAway"] == "home")
        away = next(c for c in competitors if c["homeAway"] == "away")
        return ScheduledGame(
            game_date=game_date,
            home_team=home["team"]["displayName"],
            away_team=away["team"]["displayName"],
            league=league,
            game_id=event.get("id", ""),
            venue=event.get("competitions", [{}])[0]
                      .get("venue", {}).get("fullName", ""),
        )
    except (KeyError, StopIteration, IndexError):
        return None

"""
Gap analysis: find official scheduled games that are absent from inventory.

Matching logic
--------------
A scheduled game is considered "covered" when, on the same calendar date,
there is at least one inventory event where either the home team name or the
away team name has a significant word (≥ min_word_len chars) that appears in
the event's combined text (raw_title + home_team + away_team).

This intentionally tolerates minor name differences such as:
  Official: "Los Angeles Dodgers"  →  Inventory: "LA Dodgers"
  Official: "Toronto Maple Leafs" →  Inventory: "Maple Leafs"
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date

from ..leagues import team_in_text
from ..models import Event, ScheduledGame

logger = logging.getLogger(__name__)


def find_missing_games(
    inventory: list[Event],
    schedule: list[ScheduledGame],
    league: str,
    min_word_len: int = 4,
) -> list[ScheduledGame]:
    """
    Return the subset of *schedule* not found in *inventory*.

    Parameters
    ----------
    inventory   : combined events from SG + TE, already filtered to the league
    schedule    : official games from today → season end
    league      : used only for logging
    min_word_len: minimum word length to use for name matching
    """
    # Build a date → [events] index for O(1) lookup per date
    by_date: dict[date, list[Event]] = defaultdict(list)
    for event in inventory:
        by_date[event.event_date].append(event)

    missing: list[ScheduledGame] = []

    for game in schedule:
        day_events = by_date.get(game.game_date, [])
        if not _is_covered(game, day_events, min_word_len):
            missing.append(game)

    covered = len(schedule) - len(missing)
    logger.info(
        "[%s] Schedule: %d games | Covered: %d | Missing: %d",
        league,
        len(schedule),
        covered,
        len(missing),
    )
    return missing


def _is_covered(game: ScheduledGame, day_events: list[Event], min_word_len: int) -> bool:
    """Return True if any inventory event on the same day matches the game."""
    for event in day_events:
        text = event.team_text()
        if team_in_text(game.home_team, text, min_word_len) or \
           team_in_text(game.away_team, text, min_word_len):
            return True
    return False


def summarise_coverage(
    inventory: list[Event],
    schedule: list[ScheduledGame],
    league: str,
) -> dict:
    """Return a coverage summary dict (used for the terminal status table)."""
    missing = find_missing_games(inventory, schedule, league)
    return {
        "league": league,
        "scheduled": len(schedule),
        "covered": len(schedule) - len(missing),
        "missing": len(missing),
        "missing_games": missing,
    }

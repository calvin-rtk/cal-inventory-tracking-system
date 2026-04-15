"""
CSV report writer.

Produces two files per run:
  output/<league>_missing_<timestamp>.csv   — games absent from both tools
  output/<league>_inventory_<timestamp>.csv — all events found in tools for the league
"""
from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

from ..models import Event, ScheduledGame

logger = logging.getLogger(__name__)

_OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "output"


def write_missing_report(
    missing_games: list[ScheduledGame],
    league: str,
    output_dir: Path | None = None,
) -> Path:
    """
    Write missing games to CSV and return the file path.

    Columns: Date, Away Team, Home Team, League, Venue, Game ID
    """
    out_dir = output_dir or _OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = out_dir / f"{league.lower()}_missing_{timestamp}.csv"

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Date", "Away Team", "Home Team", "League", "Venue", "Game ID"])
        for game in sorted(missing_games, key=lambda g: g.game_date):
            writer.writerow([
                game.game_date.strftime("%Y-%m-%d"),
                game.away_team,
                game.home_team,
                game.league,
                game.venue,
                game.game_id,
            ])

    logger.info("Missing report written: %s  (%d rows)", path, len(missing_games))
    return path


def write_inventory_report(
    events: list[Event],
    league: str,
    output_dir: Path | None = None,
) -> Path:
    """
    Write the full inventory snapshot for a league to CSV.

    Columns: Date, Event Title, Home Team, Away Team, League, Venue, Source
    """
    out_dir = output_dir or _OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = out_dir / f"{league.lower()}_inventory_{timestamp}.csv"

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Date", "Event Title", "Home Team", "Away Team",
                          "League", "Venue", "Source"])
        for event in sorted(events, key=lambda e: e.event_date):
            writer.writerow([
                event.event_date.strftime("%Y-%m-%d"),
                event.raw_title,
                event.home_team,
                event.away_team,
                event.league,
                event.venue,
                event.source,
            ])

    logger.info("Inventory report written: %s  (%d rows)", path, len(events))
    return path

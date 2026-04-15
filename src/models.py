"""Shared data models."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class Event:
    """A single event scraped from the SG or TE tool."""

    event_date: date
    raw_title: str          # Full text as it appears in the tool
    home_team: str = ""
    away_team: str = ""
    league: str = ""        # Detected league (MLB / NHL / NBA / NFL / CONCERT / UNKNOWN)
    venue: str = ""
    source: str = ""        # "SG" | "TE"

    def team_text(self) -> str:
        """All team-related text in one searchable string."""
        return f"{self.home_team} {self.away_team} {self.raw_title}".lower()


@dataclass
class ScheduledGame:
    """A game from an official league schedule API."""

    game_date: date
    home_team: str
    away_team: str
    league: str
    game_id: str = ""
    venue: str = ""

    def label(self) -> str:
        return f"{self.away_team} @ {self.home_team}  [{self.game_date}]"

"""
Scraper for the SG events-overview page.

URL: https://tradeinternet.net/roman/sg/events-overview.php

Expected page structure: an HTML table listing all current/future events with
at minimum a date column and an event-name column.  Column layout is
auto-detected from header text; fallback indices are configurable in
config.yaml under tools.sg.scraping.
"""
from __future__ import annotations

import logging
from datetime import date

from ..auth.client import AuthenticatedClient
from ..leagues import detect_league
from ..models import Event
from .base import BaseScraper

logger = logging.getLogger(__name__)


class SGScraper(BaseScraper):
    SOURCE_NAME = "SG"

    def __init__(self, client: AuthenticatedClient, tool_config: dict) -> None:
        super().__init__(client, tool_config)
        self._events_url: str = tool_config["events_url"]

    def fetch_events(self, events_url: str | None = None) -> list[Event]:
        """
        Scrape the events-overview page and return a list of Event objects
        for today and future dates only.
        """
        url = events_url or self._events_url
        logger.info("[SG] Fetching events from: %s", url)

        soup = self._fetch_soup(url)
        table = self._select_table(soup)
        if table is None:
            logger.error(
                "[SG] No event table found. Run `python main.py discover` to "
                "inspect the page and update config.yaml → tools.sg.scraping."
            )
            return []

        headers = self._extract_headers(table)
        col = self._detect_column_indices(headers)
        logger.debug("[SG] Column mapping: %s  (headers: %s)", col, headers)

        events: list[Event] = []
        today = date.today()
        skipped_past = 0

        for row in self._iter_data_rows(table):
            raw_date = self._cell_text(row, col["date"])
            raw_event = self._cell_text(row, col["event"])
            raw_venue = self._cell_text(row, col.get("venue", 2))

            if not raw_date and not raw_event:
                continue

            event_date = self._parse_date(raw_date) or self._row_date_fallback(row)
            if event_date is None:
                logger.debug("[SG] Skipping row — unparseable date %r", raw_date)
                continue
            if event_date < today:
                skipped_past += 1
                continue

            home, away = self._parse_teams(raw_event)
            league = detect_league(raw_event)

            events.append(
                Event(
                    event_date=event_date,
                    raw_title=raw_event,
                    home_team=home,
                    away_team=away,
                    league=league,
                    venue=raw_venue,
                    source="SG",
                )
            )

        logger.info(
            "[SG] Found %d future events (skipped %d past).",
            len(events),
            skipped_past,
        )
        return events

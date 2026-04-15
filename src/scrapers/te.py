"""
Scraper for the TE events-overview page.

URL: https://tradeinternet.net/roman/te/events-overview.php

Same table structure as the SG overview: Table #1 is the events data table
with an 'event name' column that embeds the date in the cell text.
"""
from __future__ import annotations

import logging
from datetime import date

from bs4 import BeautifulSoup, Tag

from ..auth.client import AuthenticatedClient
from ..leagues import detect_league
from ..models import Event
from .base import BaseScraper

logger = logging.getLogger(__name__)


class TEScraper(BaseScraper):
    SOURCE_NAME = "TE"

    def __init__(self, client: AuthenticatedClient, tool_config: dict) -> None:
        super().__init__(client, tool_config)
        self._events_url: str = tool_config["events_url"]

    def fetch_events(self, events_url: str | None = None) -> list[Event]:
        url = events_url or self._events_url
        logger.info("[TE] Fetching events from: %s", url)

        soup = self._fetch_soup(url)

        # Try venue-grouped layout first, fall back to flat table
        events = self._parse_grouped(soup)
        if not events:
            logger.debug("[TE] Grouped layout yielded no events; trying flat table.")
            events = self._parse_flat(soup)

        if not events:
            logger.error(
                "[TE] No events found. Run `python main.py discover` to "
                "inspect the page and update config.yaml → tools.te.scraping."
            )

        today = date.today()
        future_events = [e for e in events if e.event_date >= today]
        skipped = len(events) - len(future_events)
        logger.info(
            "[TE] Found %d future events (skipped %d past).",
            len(future_events),
            skipped,
        )
        return future_events

    # ------------------------------------------------------------------
    # Layout parsers
    # ------------------------------------------------------------------

    def _parse_flat(self, soup: BeautifulSoup) -> list[Event]:
        """Parse a single flat <table> of events."""
        table = self._select_table(soup)
        if table is None:
            return []

        headers = self._extract_headers(table)
        col = self._detect_column_indices(headers)
        logger.debug("[TE] Flat table — column mapping: %s", col)

        events: list[Event] = []
        for row in self._iter_data_rows(table):
            event = self._row_to_event(row, col, venue="")
            if event:
                events.append(event)
        return events

    def _parse_grouped(self, soup: BeautifulSoup) -> list[Event]:
        """
        Parse a venue-grouped layout:
            <h2>Venue Name</h2>
            <table>... events ...</table>
        """
        events: list[Event] = []
        for heading in soup.find_all(["h2", "h3"]):
            venue_name = heading.get_text(strip=True)
            # Find the next sibling table
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in ("table", "h2", "h3"):
                sibling = sibling.find_next_sibling()
            if sibling and sibling.name == "table":
                headers = self._extract_headers(sibling)
                col = self._detect_column_indices(headers)
                for row in self._iter_data_rows(sibling):
                    event = self._row_to_event(row, col, venue=venue_name)
                    if event:
                        events.append(event)
        return events

    # ------------------------------------------------------------------
    # Row conversion
    # ------------------------------------------------------------------

    def _row_to_event(self, row: Tag, col: dict, venue: str) -> Event | None:
        raw_date = self._cell_text(row, col["date"])
        raw_event = self._cell_text(row, col["event"])
        raw_venue = self._cell_text(row, col.get("venue", 2)) or venue

        if not raw_date and not raw_event:
            return None

        event_date = self._parse_date(raw_date) or self._row_date_fallback(row)
        if event_date is None:
            logger.debug("[TE] Skipping row — unparseable date %r", raw_date)
            return None

        home, away = self._parse_teams(raw_event)
        league = detect_league(raw_event)

        return Event(
            event_date=event_date,
            raw_title=raw_event,
            home_team=home,
            away_team=away,
            league=league,
            venue=raw_venue,
            source="TE",
        )

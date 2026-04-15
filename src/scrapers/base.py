"""
Base scraper with shared table-parsing logic.

Both SG and TE pages are expected to expose one or more HTML <table> elements.
The base class provides:
  - _fetch_soup()      — authenticated GET → BeautifulSoup
  - _parse_table()     — extract rows from a <table>, auto-detecting or
                         falling back to configured column indices
  - discover_tables()  — print table structures for configuration help
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Generator
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from ..auth.client import AuthenticatedClient
from ..models import Event

logger = logging.getLogger(__name__)

# Header keywords used to auto-detect column roles
_DATE_KEYWORDS = {"date", "day", "when", "game date", "event date"}
_EVENT_KEYWORDS = {"event", "name", "title", "description", "matchup", "game"}
_VENUE_KEYWORDS = {"venue", "location", "arena", "stadium", "place"}

# Regex patterns for extracting a date from arbitrary cell text
# (handles cells like "2026-04-14 06:40pm Cubs at Phillies - TODAY")
_DATE_REGEXES: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\b(\d{4}-\d{1,2}-\d{1,2})\b'),          "%Y-%m-%d"),
    (re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b'),           "%m/%d/%Y"),
    (re.compile(r'\b(\d{1,2}/\d{1,2}/\d{2})\b'),           "%m/%d/%y"),
    (re.compile(r'\b(\w+ \d{1,2},? \d{4})\b'),             "%B %d, %Y"),
    (re.compile(r'\b(\w{3} \d{1,2},? \d{4})\b'),           "%b %d, %Y"),
]


class BaseScraper:
    SOURCE_NAME: str = "BASE"

    def __init__(self, client: AuthenticatedClient, tool_config: dict) -> None:
        self.client = client
        self.cfg = tool_config["scraping"]

    # ------------------------------------------------------------------
    # Public interface (override in subclasses if needed)
    # ------------------------------------------------------------------

    def fetch_events(self, events_url: str) -> list[Event]:
        raise NotImplementedError

    def discover_tables(self, url: str) -> list[dict]:
        """
        Return a list of dicts describing every <table> found on *url*.
        Used by the `discover` CLI command — does not require a specific
        column layout to be configured.
        """
        soup = self._fetch_soup(url)
        results = []
        for idx, table in enumerate(soup.find_all("table")):
            headers = self._extract_headers(table)
            sample_rows = list(self._iter_data_rows(table))[:3]
            sample_text = [
                [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                for row in sample_rows
            ]
            results.append(
                {
                    "table_index": idx,
                    "headers": headers,
                    "row_count": len(list(self._iter_data_rows(table))),
                    "sample_rows": sample_text,
                }
            )
        return results

    # ------------------------------------------------------------------
    # Shared parsing helpers
    # ------------------------------------------------------------------

    def _fetch_soup(self, url: str) -> BeautifulSoup:
        resp = self.client.get(url)
        return BeautifulSoup(resp.text, "lxml")

    def _select_table(self, soup: BeautifulSoup) -> Tag | None:
        tables = soup.select(self.cfg.get("table_selector", "table"))
        if not tables:
            logger.warning("[%s] No tables matched selector '%s'.",
                           self.SOURCE_NAME, self.cfg.get("table_selector"))
            return None
        idx = self.cfg.get("table_index", 0)
        if idx >= len(tables):
            logger.warning("[%s] table_index %d out of range (%d tables found); using last.",
                           self.SOURCE_NAME, idx, len(tables))
            idx = len(tables) - 1
        return tables[idx]

    def _extract_headers(self, table: Tag) -> list[str]:
        """Return header cell text from the first <tr> that contains <th> elements."""
        for row in table.find_all("tr"):
            cells = row.find_all("th")
            if cells:
                return [c.get_text(strip=True).lower() for c in cells]
        # No <th> found — use first row's <td> text
        first_row = table.find("tr")
        if first_row:
            return [c.get_text(strip=True).lower()
                    for c in first_row.find_all("td")]
        return []

    def _detect_column_indices(self, headers: list[str]) -> dict[str, int]:
        """Map role → column index by matching header keywords."""
        mapping: dict[str, int] = {}
        for i, h in enumerate(headers):
            if h in _DATE_KEYWORDS and "date" not in mapping:
                mapping["date"] = i
            elif any(k in h for k in _EVENT_KEYWORDS) and "event" not in mapping:
                mapping["event"] = i
            elif any(k in h for k in _VENUE_KEYWORDS) and "venue" not in mapping:
                mapping["venue"] = i
        # Fall back to configured indices for any missing role
        mapping.setdefault("date", self.cfg.get("fallback_date_col", 0))
        mapping.setdefault("event", self.cfg.get("fallback_event_col", 1))
        mapping.setdefault("venue", self.cfg.get("fallback_venue_col", 2))
        return mapping

    def _iter_data_rows(self, table: Tag) -> Generator[Tag, None, None]:
        """Yield all <tr> rows that contain <td> elements (skips header rows)."""
        for row in table.find_all("tr"):
            if row.find("td"):
                yield row

    def _parse_date(self, raw: str) -> date | None:
        """
        Extract a date from *raw* cell text.

        Strategy (in order):
        1. Exact match against configured + common formats.
        2. Regex extraction — finds the first date-shaped substring in a
           cell that also contains a time, label, or full event text.
        """
        text = raw.strip()
        fmt = self.cfg.get("date_format", "%m/%d/%Y")

        # 1. Exact format match (fast path)
        for candidate in [fmt, "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"]:
            try:
                return datetime.strptime(text, candidate).date()
            except ValueError:
                continue

        # 2. Regex extraction (handles "2026-04-14 06:40pm Some Event - TODAY")
        for pattern, fmt_str in _DATE_REGEXES:
            m = pattern.search(text)
            if m:
                date_str = m.group(1).rstrip(",")
                try:
                    return datetime.strptime(date_str, fmt_str).date()
                except ValueError:
                    continue

        logger.debug("Could not parse date from: %r", raw)
        return None

    def _row_date_fallback(self, row: Tag) -> date | None:
        """
        Scan every cell in a row looking for a parseable date.
        Used when the configured date column contains no parseable date.
        """
        for cell in row.find_all("td"):
            d = self._parse_date(cell.get_text(separator=" ", strip=True))
            if d is not None:
                return d
        return None

    def _cell_text(self, row: Tag, index: int) -> str:
        cells = row.find_all("td")
        if index < len(cells):
            return cells[index].get_text(separator=" ", strip=True)
        return ""

    def _parse_teams(self, event_text: str) -> tuple[str, str]:
        """
        Attempt to split 'Away @ Home' or 'Home vs Away' into component teams.
        Returns (home, away) or (event_text, "") when no separator is found.
        """
        for sep in [" @ ", " vs. ", " vs ", " at ", " @ "]:
            if sep in event_text:
                parts = event_text.split(sep, 1)
                if sep in (" @ ",):
                    return parts[1].strip(), parts[0].strip()  # home, away
                else:
                    return parts[0].strip(), parts[1].strip()  # home, away
        return event_text, ""

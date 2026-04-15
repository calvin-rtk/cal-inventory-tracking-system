"""
Scraper that checks for "all-7's" pricing across events.

Pricing URL pattern (both tools):
    {base_url}/update-prices.php?event_id={id}

Strategy for finding the event_id from each overview row (in priority order):
  1. Find a link directly to update-prices.php → use it as-is
  2. Find any link with event_id= in the query string → rewrite to update-prices.php
  3. Scan cells for a long numeric string (TE multi-value: "4647370\\n11459953"
     → take only the first number) → construct URL
  4. Heuristic link selection (last resort / fallback)
"""
from __future__ import annotations

import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from ..auth.client import AuthenticatedClient
from .base import BaseScraper

logger = logging.getLogger(__name__)

# Matches any run of 5+ digits (event IDs are typically 7-10 digits)
_ID_RE = re.compile(r"\d{5,}")

# Matches: 7  7.0  7.00  $7  $7.00  $ 7  7,00  etc.
_SEVEN_RE = re.compile(r"^\$?\s*7[\.,]?0*\s*$")

# Icon class fragments that suggest a money/pricing icon (fallback heuristic)
_MONEY_ICON_KEYWORDS = {
    "money", "dollar", "usd", "cash", "price", "currency", "coin",
    "chart", "trending", "fa-line-chart", "fa-money", "fa-usd", "glyphicon-usd",
}


class SevensScraper(BaseScraper):
    SOURCE_NAME = "SEVENS"

    def __init__(self, client: AuthenticatedClient, tool_config: dict) -> None:
        super().__init__(client, tool_config)
        self._events_url: str = tool_config["events_url"]
        self._base_url: str = tool_config["base_url"].rstrip("/")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_check(self, progress_cb=None) -> list[dict]:
        """
        Full 7's check across all events on the overview page.

        Optional progress_cb(done: int, total: int) is called after each event.

        Returns a list of result dicts:
            {
              "event":         str,
              "date":          str,
              "pricing_url":   str,
              "is_all_sevens": bool,
              "seven_count":   int,
              "total_rows":    int,
              "prices":        list[str],
              "fetch_ok":      bool,
              "column_found":  bool,
              "column_header": str,
            }
        """
        event_links = self._extract_event_links()
        results = []
        total = len(event_links)
        for i, item in enumerate(event_links):
            check = self._check_pricing_page(item["pricing_url"])
            results.append({**item, **check})
            if progress_cb:
                progress_cb(i + 1, total)
        return results

    def discover_event_links(self) -> list[dict]:
        """
        Return all links found per event row (for Discover / debug mode).
        Each entry: {"event", "date", "resolved_pricing_url", "links": [...]}
        """
        return self._extract_event_links(all_links=True)

    # ------------------------------------------------------------------
    # Internal — event link extraction
    # ------------------------------------------------------------------

    def _extract_event_links(self, all_links: bool = False) -> list[dict]:
        soup = self._fetch_soup(self._events_url)
        table = self._select_table(soup)
        if table is None:
            logger.warning("[SEVENS] No overview table found.")
            return []

        headers = self._extract_headers(table)
        col = self._detect_column_indices(headers)

        results = []
        for row in self._iter_data_rows(table):
            raw_date = self._cell_text(row, col["date"])
            raw_event = self._cell_text(row, col["event"])
            if not raw_event:
                continue

            event_date = self._parse_date(raw_date) or self._row_date_fallback(row)
            pricing_url = self._find_pricing_url(row)

            if all_links:
                results.append({
                    "event": raw_event,
                    "date": str(event_date) if event_date else "",
                    "resolved_pricing_url": pricing_url or "",
                    "links": self._find_row_links(row),
                })
            elif pricing_url:
                results.append({
                    "event": raw_event,
                    "date": str(event_date) if event_date else "",
                    "pricing_url": pricing_url,
                })

        return results

    def _find_pricing_url(self, row: Tag) -> str | None:
        """
        Resolve the update-prices URL for a row using a 4-level strategy.
        """
        # Strategy 1 & 2: scan anchor tags for event_id or update-prices path
        for a in row.find_all("a", href=True):
            href = a["href"]
            if not href or href.startswith("#") or href.lower().startswith("javascript"):
                continue
            full_url = urljoin(self._base_url + "/", href)
            parsed = urlparse(full_url)
            params = parse_qs(parsed.query)

            # Already a direct update-prices link
            if "update-prices.php" in parsed.path:
                return full_url

            # Any link carrying event_id → rewrite to update-prices.php
            if "event_id" in params:
                event_id = params["event_id"][0]
                return f"{self._base_url}/update-prices.php?event_id={event_id}"

        # Strategy 3: scan cells for a numeric event ID
        event_id = self._extract_id_from_cells(row)
        if event_id:
            return f"{self._base_url}/update-prices.php?event_id={event_id}"

        # Strategy 4: heuristic link selection (last resort)
        row_links = self._find_row_links(row)
        return self._pick_pricing_link_heuristic(row_links)

    def _extract_id_from_cells(self, row: Tag) -> str | None:
        """
        Scan every cell for a 5+ digit number (event ID).
        For TE multi-value cells like "4647370\\n11459953", returns the FIRST number.
        """
        for cell in row.find_all("td"):
            text = cell.get_text(separator=" ", strip=True)
            m = _ID_RE.search(text)
            if m:
                return m.group(0)
        return None

    # ------------------------------------------------------------------
    # Internal — link helpers (used by discover mode + strategy 4)
    # ------------------------------------------------------------------

    def _find_row_links(self, row: Tag) -> list[dict]:
        """Return metadata for every <a href> in a table row."""
        links = []
        for a in row.find_all("a", href=True):
            href = a["href"]
            if not href or href.startswith("#") or href.lower().startswith("javascript"):
                continue
            full_url = urljoin(self._base_url + "/", href)
            title = a.get("title", "") or ""
            text = a.get_text(strip=True)

            icon_classes: list[str] = []
            for child in a.find_all(True):
                cls = child.get("class", [])
                if isinstance(cls, list):
                    icon_classes.extend(cls)
                child_title = child.get("title") or child.get("alt") or ""
                if child_title and not title:
                    title = child_title

            is_money_icon = any(
                kw in cls_str
                for cls_str in [" ".join(icon_classes).lower(), title.lower()]
                for kw in _MONEY_ICON_KEYWORDS
            )

            links.append({
                "url": full_url,
                "text": text,
                "title": title,
                "icon_classes": icon_classes,
                "is_money_icon": is_money_icon,
            })
        return links

    def _pick_pricing_link_heuristic(self, links: list[dict]) -> str | None:
        """Last-resort: pick based on money icon class, URL keyword, or last link."""
        if not links:
            return None
        for link in links:
            if link.get("is_money_icon"):
                return link["url"]
        _URL_KEYWORDS = {"price", "pricing", "ticket", "detail", "buy", "update"}
        for link in links:
            if any(kw in link["url"].lower() for kw in _URL_KEYWORDS):
                return link["url"]
        return links[-1]["url"]

    # ------------------------------------------------------------------
    # Internal — pricing page analysis
    # ------------------------------------------------------------------

    def _check_pricing_page(self, url: str) -> dict:
        """
        Fetch a pricing page, find the "Current buy price" column,
        and return whether every price is exactly $7.
        """
        try:
            soup = self._fetch_soup(url)
        except Exception as exc:
            logger.warning("[SEVENS] Fetch failed for %s: %s", url, exc)
            return {
                "is_all_sevens": False, "seven_count": 0, "total_rows": 0,
                "prices": [], "fetch_ok": False, "column_found": False,
                "column_header": "", "error": str(exc),
            }

        buy_col, target_table, matched_header = self._find_buy_price_column(soup)
        if target_table is None or buy_col is None:
            all_headers = [self._extract_headers(t) for t in soup.find_all("table")]
            return {
                "is_all_sevens": False, "seven_count": 0, "total_rows": 0,
                "prices": [], "fetch_ok": True, "column_found": False,
                "column_header": "", "all_table_headers": all_headers,
            }

        prices = [
            self._cell_text(row, buy_col)
            for row in self._iter_data_rows(target_table)
            if self._cell_text(row, buy_col)
        ]

        seven_count = sum(1 for p in prices if _SEVEN_RE.match(p.strip()))
        total = len(prices)
        seven_pct = round(seven_count / total * 100, 1) if total > 0 else 0.0

        return {
            "is_all_sevens": total > 0 and seven_pct >= 75.0,
            "seven_count": seven_count,
            "seven_pct": seven_pct,
            "total_rows": total,
            "prices": prices,
            "fetch_ok": True,
            "column_found": True,
            "column_header": matched_header,
        }

    def _find_buy_price_column(
        self, soup: BeautifulSoup
    ) -> tuple[int | None, Tag | None, str]:
        """
        Scan all tables for a "Current buy price" (or similar) column.
        Returns (col_index, table, matched_header) or (None, None, "").
        """
        _BUY_KEYWORDS = [
            "current buy", "buy price", "buy now", "current price",
            "curr buy", "cbp", "buy",
        ]
        for table in soup.find_all("table"):
            headers = self._extract_headers(table)
            for i, h in enumerate(headers):
                for kw in _BUY_KEYWORDS:
                    if kw in h:
                        return i, table, h
        return None, None, ""

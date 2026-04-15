"""
Authenticated HTTP session for one tool instance.

Handles:
  - Login form detection (including hidden CSRF fields)
  - Session cookie persistence across requests
  - Login success/failure verification
  - A discover() helper that returns form fields and login diagnostics
"""
from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class AuthenticatedClient:
    """Manages a logged-in requests.Session for a single tool."""

    def __init__(
        self,
        tool_name: str,
        base_url: str,
        login_url: str,
        username: str,
        password: str,
        tool_config: dict,
    ) -> None:
        self.tool_name = tool_name
        self.base_url = base_url.rstrip("/")
        self.login_url = login_url
        self.username = username
        self.password = password
        self._form_cfg = tool_config["form"]
        self._indicators = tool_config["login_indicators"]

        self.session = requests.Session()
        self.session.headers.update(_DEFAULT_HEADERS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def login(self) -> None:
        """
        Fetch the login page, collect all form fields (including hidden
        CSRF tokens), inject credentials, and POST.  Raises RuntimeError
        on failure.
        """
        logger.info("[%s] Fetching login page: %s", self.tool_name, self.login_url)
        resp = self.session.get(self.login_url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        form = soup.find("form")
        if not form:
            raise RuntimeError(
                f"[{self.tool_name}] No <form> found on login page {self.login_url}."
            )

        # Collect all existing input values (includes hidden/CSRF fields)
        payload: dict[str, str] = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                payload[name] = inp.get("value", "")

        u_field = self._form_cfg["username_field"]
        p_field = self._form_cfg["password_field"]

        # Warn early if the configured field names aren't in the form
        if u_field not in payload:
            logger.warning(
                "[%s] username_field '%s' not found in login form. "
                "Available fields: %s",
                self.tool_name, u_field, list(payload.keys()),
            )
        if p_field not in payload:
            logger.warning(
                "[%s] password_field '%s' not found in login form. "
                "Available fields: %s",
                self.tool_name, p_field, list(payload.keys()),
            )

        payload[u_field] = self.username
        payload[p_field] = self.password

        action = form.get("action") or self.login_url
        post_url = urljoin(self.base_url + "/", action)

        logger.info("[%s] Posting credentials to: %s", self.tool_name, post_url)
        resp = self.session.post(post_url, data=payload, timeout=30, allow_redirects=True)
        resp.raise_for_status()

        self._verify_login(resp)
        logger.info("[%s] Login successful.", self.tool_name)

    def get(self, url: str) -> requests.Response:
        """Perform an authenticated GET and return the response."""
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp

    def discover(self) -> dict:
        """
        Fetch the login page and return all form field names/types
        along with a mismatch check against the current config.
        Does NOT log in.
        """
        resp = self.session.get(self.login_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        form = soup.find("form")
        if not form:
            return {"error": "No <form> found on login page."}

        fields = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                fields[name] = {
                    "type": inp.get("type", "text"),
                    "value": inp.get("value", ""),
                }

        u_field = self._form_cfg["username_field"]
        p_field = self._form_cfg["password_field"]

        mismatches = []
        if u_field not in fields:
            mismatches.append(
                f"username_field is '{u_field}' but form fields are: {list(fields.keys())}"
            )
        if p_field not in fields:
            mismatches.append(
                f"password_field is '{p_field}' but form fields are: {list(fields.keys())}"
            )

        return {
            "form_action": form.get("action", "(none)"),
            "form_method": form.get("method", "get").upper(),
            "fields": fields,
            "config_username_field": u_field,
            "config_password_field": p_field,
            "mismatches": mismatches,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _verify_login(self, resp: requests.Response) -> None:
        """
        Raise RuntimeError if we can detect the login failed.
        Checks (in order):
          1. Explicit failure marker in body
          2. Final URL still looks like the login page
          3. Response body still contains a password input (still on login page)
          4. Success marker absent (raises, not just a warning)
        """
        body = resp.text
        final_url = resp.url

        failure_marker = self._indicators.get("failure_marker", "")
        success_marker = self._indicators.get("success_marker", "")

        # 1. Explicit failure text
        if failure_marker and failure_marker.lower() in body.lower():
            raise RuntimeError(
                f"[{self.tool_name}] Login failed — server returned failure marker "
                f"'{failure_marker}'. Check credentials in .env."
            )

        # 2. Still on the login URL
        if "login" in urlparse(final_url).path.lower():
            raise RuntimeError(
                f"[{self.tool_name}] Login failed — redirected back to login page "
                f"({final_url}). Check that username_field / password_field in "
                f"config.yaml match the actual form field names."
            )

        # 3. Response still has a password field → still on login page
        soup = BeautifulSoup(body, "lxml")
        if soup.find("input", {"type": "password"}):
            raise RuntimeError(
                f"[{self.tool_name}] Login failed — response still contains a "
                f"password field. The form field names in config.yaml may be wrong. "
                f"Run Discover to check them."
            )

        # 4. Success marker missing
        if success_marker and success_marker.lower() not in body.lower():
            raise RuntimeError(
                f"[{self.tool_name}] Login failed — success marker '{success_marker}' "
                f"not found in response. Check credentials or update "
                f"login_indicators.success_marker in config.yaml."
            )

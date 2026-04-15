"""Load YAML config and .env credentials into typed dicts."""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    config_path = _ROOT / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {config_path}")
    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    # URLs are kept out of config.yaml and loaded from the environment
    # so the repo can be public without leaking internal tool addresses.
    load_dotenv(dotenv_path=_ROOT / ".env")

    def _require_url(key: str) -> str:
        val = os.getenv(key, "").strip().rstrip("/")
        if not val:
            raise EnvironmentError(
                f"Missing URL environment variable: {key}\n"
                "Add it to .env (local) or Streamlit secrets (cloud)."
            )
        return val

    cfg["tools"]["sg"]["base_url"]   = _require_url("SG_BASE_URL")
    cfg["tools"]["sg"]["login_url"]  = _require_url("SG_LOGIN_URL")
    cfg["tools"]["sg"]["events_url"] = _require_url("SG_EVENTS_URL")
    cfg["tools"]["te"]["base_url"]   = _require_url("TE_BASE_URL")
    cfg["tools"]["te"]["login_url"]  = _require_url("TE_LOGIN_URL")
    cfg["tools"]["te"]["events_url"] = _require_url("TE_EVENTS_URL")

    return cfg


def load_credentials() -> dict[str, dict[str, str]]:
    """
    Load SG and TE credentials from .env.
    Returns:
        {
            "sg": {"username": "...", "password": "..."},
            "te": {"username": "...", "password": "..."},
        }
    """
    env_path = _ROOT / ".env"
    load_dotenv(dotenv_path=env_path)

    def _require(key: str) -> str:
        val = os.getenv(key, "").strip()
        if not val:
            raise EnvironmentError(
                f"Missing credential: {key}\n"
                f"Copy .env.example → .env and fill in all values."
            )
        return val

    return {
        "sg": {
            "username": _require("SG_USERNAME"),
            "password": _require("SG_PASSWORD"),
        },
        "te": {
            "username": _require("TE_USERNAME"),
            "password": _require("TE_PASSWORD"),
        },
    }

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
        return yaml.safe_load(fh)


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

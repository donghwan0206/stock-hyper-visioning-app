from __future__ import annotations

import os
from pathlib import Path

import pytest

from kis_api.client import KISClient


def _load_dotenv() -> None:
    """Populate os.environ using the closest .env file (if any)."""
    env_loaded_flag = "_KIS_ENV_LOADED"
    if os.environ.get(env_loaded_flag):
        return

    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        env_path = parent / ".env"
        if env_path.is_file():
            for raw_line in env_path.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                normalized = value.strip().strip('"').strip("'")
                os.environ.setdefault(key.strip(), normalized)
            os.environ[env_loaded_flag] = "1"
            break


@pytest.fixture(scope="session")
def kis_client() -> KISClient:
    """Provide an authenticated KISClient configured via environment variables."""
    _load_dotenv()
    app_key = os.getenv("KIS_APP_KEY")
    app_secret = os.getenv("KIS_APP_SECRET")
    if not app_key or not app_secret:
        pytest.skip("KIS_APP_KEY and KIS_APP_SECRET must be set (e.g. via .env)")
    base_url = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
    timeout = float(os.getenv("KIS_TIMEOUT", "10"))
    return KISClient(app_key=app_key, app_secret=app_secret, base_url=base_url, timeout=timeout)

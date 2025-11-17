from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import timezone, timedelta
from typing import Any, Mapping, MutableMapping, Optional

import httpx

__all__ = ["KISClient", "KST", "DEFAULT_HEADERS"]

KST = timezone(timedelta(hours=9))
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
}


@dataclass
class KISClient:
    """Minimal client that handles token issuance and authenticated requests."""

    app_key: str
    app_secret: str
    base_url: str = "https://openapi.koreainvestment.com:9443"
    timeout: float = 10.0

    _token_expires_at: float = field(default=0.0, init=False, repr=False)
    _access_token: Optional[str] = field(default=None, init=False, repr=False)

    def _issue_token(self) -> None:
        """Fetch a new access token when none exists or it is expired."""
        url = f"{self.base_url}/oauth2/tokenP"
        payload: MutableMapping[str, Any] = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        # Renew five minutes before expiration to avoid race conditions.
        self._token_expires_at = time.time() + int(data["expires_in"]) - 300
        logging.debug("KIS access token updated, expires_at=%s", self._token_expires_at)

    def _auth_headers(self) -> Mapping[str, str]:
        if not self._access_token or time.time() >= self._token_expires_at:
            self._issue_token()
        return {
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """Common request helper that injects auth headers and returns JSON."""
        url = f"{self.base_url}{path}"
        merged_headers = {**DEFAULT_HEADERS, **self._auth_headers(), **(headers or {})}
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.request(method.upper(), url, params=params, json=json, headers=merged_headers)
            resp.raise_for_status()
        return resp.json()

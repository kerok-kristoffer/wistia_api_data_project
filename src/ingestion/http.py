import time
import requests
from typing import Any, Dict


class WistiaError(Exception): ...


class AuthError(WistiaError): ...


class NotFound(WistiaError): ...


class WistiaClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_s: float = 15.0,
        session: requests.Session | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session = session or requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        backoff = 1.0
        for _ in range(5):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_s)
            except requests.exceptions.Timeout:
                # Treat timeouts like transient errors and retry
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as e:
                    raise WistiaError("Invalid JSON in response") from e
            if resp.status_code == 401:
                raise AuthError("Unauthorized")
            if resp.status_code == 404:
                raise NotFound(f"Not found: {url}")
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            raise WistiaError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        raise WistiaError("Gave up after retries")

    def media_stats(self, media_id: str, **params) -> Dict[str, Any]:
        return self._request(f"stats/medias/{media_id}.json", params=params)

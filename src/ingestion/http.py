import time
import requests
from typing import List, Any, Dict


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

    def _request(self, path: str, params: Dict[str, Any]) -> Any:
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

    def events(self, **params) -> list[Dict[str, Any]]:
        """
        Wrapper for GET /v1/stats/events
        Returns a list of event dicts (possibly empty).
        """
        data = self._request("stats/events", params=params)
        if not isinstance(data, list):
            raise WistiaError("Expected a list from /stats/events")
        return data

    def media_stats_by_date(
        self, media_id: str, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Calls: GET /stats/medias/{media_id}/by_date?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
        Returns a list of daily KPI dicts (possibly empty).
        """
        path = f"stats/medias/{media_id}/by_date"  # no leading slash needed
        params = {"start_date": start_date, "end_date": end_date}
        resp = self._request(path, params=params)  # <-- call signature fix
        if not isinstance(resp, list):
            raise WistiaError(f"Unexpected payload for by_date: {type(resp)}")
        return resp

    def stats_visitors(
        self,
        *,
        media_id: str,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        GET /v1/stats/visitors.json?media_id=<id>&page=<n>
        Returns a list of visitors for a media.

        Note: Wistia's API for this endpoint typically supports `page`; if `per_page`
        is not supported, it will be ignored by the API (we still accept it for parity).
        """
        params: Dict[str, Any] = {"media_id": media_id, "page": page}

        return self._request(
            "/stats/visitors.json",
            params=params,
            # Expect a JSON array; if the API returns an envelope, _request should already
            # normalize or you can add a small guard here if your _request is "raw".
        )

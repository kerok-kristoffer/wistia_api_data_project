import urllib.parse

import responses
import pytest
import time
import requests
from ingestion.http import WistiaClient, AuthError, NotFound, WistiaError


@responses.activate
def test_media_stats_happy_path():
    client = WistiaClient("https://api.wistia.com/v1", "fake")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(responses.GET, url, json={"plays": 10}, status=200)
    data = client.media_stats("abc")
    assert data["plays"] == 10


@responses.activate
def test_auth_error_401():
    client = WistiaClient("https://api.wistia.com/v1", "bad")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(responses.GET, url, json={"error": "x"}, status=401)
    try:
        client.media_stats("abc")
        assert False
    except AuthError:
        assert True


@responses.activate
def test_not_found_404():
    client = WistiaClient("https://api.wistia.com/v1", "fake")
    url = "https://api.wistia.com/v1/stats/medias/missing.json"
    responses.add(responses.GET, url, json={}, status=404)
    try:
        client.media_stats("missing")
        assert False
    except NotFound:
        assert True


@responses.activate
def test_retries_then_succeeds(monkeypatch):
    # Speed up backoff by stubbing sleep
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))

    client = WistiaClient("https://api.wistia.com/v1", "fake")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"

    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, json={"plays": 42}, status=200)

    data = client.media_stats("abc")

    assert data["plays"] == 42
    assert len(responses.calls) == 3
    # Backoff happened at least twice
    assert len(sleeps) >= 2


@responses.activate
def test_retries_then_gives_up(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)

    client = WistiaClient("https://api.wistia.com/v1", "fake")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"

    for _ in range(5):
        responses.add(responses.GET, url, status=500)

    with pytest.raises(WistiaError) as ei:
        client.media_stats("abc")
    assert "Gave up" in str(ei.value)
    assert len(responses.calls) == 5


@responses.activate
def test_timeout_then_success(monkeypatch):
    # Patch session.get to raise Timeout first, then let responses handle a 200
    session = requests.Session()
    client = WistiaClient("https://api.wistia.com/v1", "fake", session=session)
    url_path = "stats/medias/abc.json"
    url = f"https://api.wistia.com/v1/{url_path}"

    # First call: raise Timeout
    calls = {"n": 0}
    orig_get = session.get

    def flaky_get(*args, **kwargs):
        if calls["n"] == 0:
            calls["n"] += 1
            raise requests.exceptions.Timeout("boom")
        return orig_get(*args, **kwargs)

    monkeypatch.setattr(session, "get", flaky_get)
    responses.add(responses.GET, url, json={"ok": True}, status=200)

    data = client.media_stats("abc")
    assert data["ok"] is True


@responses.activate
def test_query_params_pass_through():
    client = WistiaClient("https://api.wistia.com/v1", "fake")
    base = "https://api.wistia.com/v1/stats/medias/abc.json"
    # Allow any query by not setting match_querystring=True
    responses.add(responses.GET, base, json={"page": 2, "per_page": 50}, status=200)

    data = client.media_stats("abc", page=2, per_page=50)
    assert data["page"] == 2

    req_url = responses.calls[0].request.url
    qs = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(req_url).query))
    assert qs["page"] == "2"
    assert qs["per_page"] == "50"


@responses.activate
def test_auth_header_set():
    client = WistiaClient("https://api.wistia.com/v1", "secret-token")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(responses.GET, url, json={}, status=200)

    client.media_stats("abc")
    auth = responses.calls[0].request.headers.get("Authorization")
    assert auth == "Bearer secret-token"


@responses.activate
def test_invalid_json_raises_wistiaerror():
    client = WistiaClient("https://api.wistia.com/v1", "fake")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(
        responses.GET, url, body="not-json", content_type="application/json", status=200
    )

    with pytest.raises(WistiaError):
        client.media_stats("abc")


@responses.activate
def test_base_url_trailing_slash():
    client = WistiaClient("https://api.wistia.com/v1/", "fake")
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(responses.GET, url, json={"ok": True}, status=200)

    data = client.media_stats("abc")
    assert data["ok"] is True
    assert responses.calls[0].request.url == url


class CapturingSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.used = False

    def get(self, *args, **kwargs):
        self.used = True
        return super().get(*args, **kwargs)


@responses.activate
def test_injected_session_is_used():
    sess = CapturingSession()
    client = WistiaClient("https://api.wistia.com/v1", "fake", session=sess)
    url = "https://api.wistia.com/v1/stats/medias/abc.json"
    responses.add(responses.GET, url, json={"ok": True}, status=200)

    client.media_stats("abc")
    assert sess.used is True

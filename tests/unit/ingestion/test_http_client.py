import responses
from ingestion.http import WistiaClient, AuthError, NotFound


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

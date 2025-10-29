import responses
from ingestion.http import WistiaClient


@responses.activate
def test_medias_get_happy_path():
    base = "https://api.wistia.com/v1"
    token = "t0k"
    c = WistiaClient(base_url=base, token=token, timeout_s=5)

    media_id = "gskhw4w4lm"
    responses.add(
        responses.GET,
        f"{base}/medias/{media_id}",
        json={"hashed_id": media_id, "duration": 123.4},
        status=200,
    )

    out = c.medias_get(media_id=media_id)
    assert out["hashed_id"] == media_id
    assert "duration" in out

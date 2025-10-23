import pytest
import responses
from ingestion.http import WistiaClient, WistiaError

BASE = "https://api.wistia.com/v1"


@responses.activate
def test_media_stats_by_date_propagates_http_error():
    client = WistiaClient(base_url=BASE, token="redacted")
    media_id = "abc123"
    day = "2025-10-20"
    url = f"{BASE}/stats/medias/{media_id}/by_date"

    # Return 500 for each retry attempt
    # If your responses version supports it, use repeat=5; otherwise loop add() 5 times
    try:
        responses.add(
            responses.GET,
            url,
            json={"error": "boom"},
            status=500,
            match=[
                responses.matchers.query_param_matcher(
                    {"start_date": day, "end_date": day}
                )
            ],
            repeat=5,  # comment this out if your responses version doesn't support it
        )
    except TypeError:
        for _ in range(5):
            responses.add(
                responses.GET,
                url,
                json={"error": "boom"},
                status=500,
                match=[
                    responses.matchers.query_param_matcher(
                        {"start_date": day, "end_date": day}
                    )
                ],
            )

    with pytest.raises(WistiaError) as exc:
        client.media_stats_by_date(media_id, day, day)

    assert "Gave up after retries" in str(exc.value)


@responses.activate
def test_media_stats_by_date_retries_then_succeeds():
    client = WistiaClient(base_url=BASE, token="redacted")
    media_id = "abc123"
    day = "2025-10-20"
    url = f"{BASE}/stats/medias/{media_id}/by_date"

    # First call 500, second call 200 with payload
    responses.add(
        responses.GET,
        url,
        json={"error": "tmp"},
        status=500,
        match=[
            responses.matchers.query_param_matcher({"start_date": day, "end_date": day})
        ],
    )
    responses.add(
        responses.GET,
        url,
        json=[{"date": day, "play_count": 4, "load_count": 21}],
        status=200,
        match=[
            responses.matchers.query_param_matcher({"start_date": day, "end_date": day})
        ],
    )

    out = client.media_stats_by_date(media_id, day, day)
    assert out and out[0]["date"] == day and out[0]["play_count"] == 4

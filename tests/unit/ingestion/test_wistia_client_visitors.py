import responses
from ingestion.http import WistiaClient


@responses.activate
def test_stats_visitors_paginates_and_returns_list():
    base = "https://api.wistia.com/v1"
    token = "t0k"
    client = WistiaClient(base_url=base, token=token, timeout_s=5)

    media_id = "abc123"

    # page 1
    responses.add(
        responses.GET,
        f"{base}/stats/visitors.json",
        json=[{"visitor_key": "v1"}, {"visitor_key": "v2"}],
        match=[
            responses.matchers.query_param_matcher({"media_id": media_id, "page": "1"})
        ],
        status=200,
    )
    # page 2 (short page)
    responses.add(
        responses.GET,
        f"{base}/stats/visitors.json",
        json=[{"visitor_key": "v3"}],
        match=[
            responses.matchers.query_param_matcher({"media_id": media_id, "page": "2"})
        ],
        status=200,
    )

    out = []
    page = 1
    while True:
        batch = client.stats_visitors(media_id=media_id, page=page)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 2:
            break
        page += 1

    assert [r["visitor_key"] for r in out] == ["v1", "v2", "v3"]

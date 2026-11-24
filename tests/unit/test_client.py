"""Client tests against the fake HubSpot API. Pure Python, no Spark."""

import pytest

from crmingest import client as client_mod
from crmingest import fake_hubspot
from crmingest.client import HubSpotClient, HubSpotError
from crmingest.fake_hubspot import TOKEN, serve


@pytest.fixture
def api(request):
    kwargs = getattr(request, "param", {})
    server, url = serve(**kwargs)
    yield url, server
    server.shutdown()


def make_client(url, token=TOKEN, **kw):
    return HubSpotClient(token, url, requests_per_second=0, sleep=lambda s: None, **kw)


def test_list_objects_follows_paging(api):
    url, _ = api
    c = make_client(url, page_size=50)
    rows = list(c.list_objects("companies", ["name"]))
    assert len(rows) == 300
    assert len({r["id"] for r in rows}) == 300
    assert c.calls == 6                                   # 300 / 50


def test_properties_are_discovered(api):
    url, _ = api
    names = {p["name"] for p in make_client(url).list_properties("deals")}
    assert {"dealname", "amount", "dealstage"} <= names


@pytest.mark.parametrize("api", [{"rate_limit_every": 3}], indirect=True)
def test_rate_limited_calls_are_retried(api):
    url, server = api
    c = make_client(url, page_size=100)
    rows = list(c.list_objects("contacts", ["email"]))
    assert len(rows) == 900                               # nothing lost to 429s
    assert c.calls > 9                                    # extra calls were the retries


def test_bad_token_fails_fast(api):
    url, _ = api
    with pytest.raises(HubSpotError, match="401"):
        list(make_client(url, token="wrong").list_objects("deals", []))


def test_search_returns_only_modified_records(api):
    url, server = api
    c = make_client(url)
    everything = list(c.list_objects("deals", ["hs_lastmodifieddate"]))
    cutoff = sorted(r["properties"]["hs_lastmodifieddate"] for r in everything)[-50]
    from crmingest.extract import to_epoch_ms
    recent = list(c.search_modified_since("deals", ["dealname"], to_epoch_ms(cutoff)))
    assert len(recent) == 50


def test_search_slices_past_the_result_cap(api, monkeypatch):
    """With a cap of 250, a 900-record search must still return every record."""
    url, _ = api
    monkeypatch.setattr(client_mod, "SEARCH_RESULT_CAP", 250)
    monkeypatch.setattr(fake_hubspot, "SEARCH_CAP", 250)
    c = make_client(url, page_size=100)
    ids = [r["id"] for r in c.search_modified_since("contacts", ["email"], 0)]
    assert set(ids) == {str(10_000 + i) for i in range(1, 901)}
    assert len(ids) >= 900                                # boundary records may repeat; MERGE dedupes


def test_archived_records_are_listed(api):
    url, server = api
    server.RequestHandlerClass.store.next_day(deletes=5)
    archived = list(make_client(url).list_objects("contacts", [], archived=True))
    assert len(archived) == 5 and all(r["archived"] for r in archived)


def test_batch_associations(api):
    url, _ = api
    links = list(make_client(url).batch_associations("deals", "companies", [str(50_001 + i) for i in range(250)]))
    assert len({l["from_id"] for l in links}) == 250
    assert all(l["to_id"] and l["type_id"] == 1 for l in links)


def test_server_errors_eventually_give_up(monkeypatch):
    class Resp:
        status_code = 503
        headers = {}
        text = "unavailable"
        content = b""

    class Session:
        headers = {}

        def request(self, *a, **k):
            return Resp()

    c = HubSpotClient("t", "http://x", max_retries=2, requests_per_second=0, session=Session(), sleep=lambda s: None)
    with pytest.raises(HubSpotError, match="503"):
        c.list_properties("deals")
    assert c.calls == 3

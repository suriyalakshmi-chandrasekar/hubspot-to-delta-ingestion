from datetime import datetime, timezone
from pathlib import Path

import pytest

from crmingest.config import load_config
from crmingest.extract import build_search_body, flatten, modified_property, select_properties, to_epoch_ms

REPO = Path(__file__).resolve().parents[2]


def test_contacts_use_their_own_modified_property():
    assert modified_property("contacts") == "lastmodifieddate"
    assert modified_property("deals") == "hs_lastmodifieddate"


@pytest.mark.parametrize("value", [
    "2026-01-01T00:00:00Z", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00+00:00",
    1767225600000, "1767225600000", datetime(2026, 1, 1, tzinfo=timezone.utc),
])
def test_to_epoch_ms(value):
    assert to_epoch_ms(value) == 1767225600000


def test_search_body_filters_sorts_and_includes_modified_property():
    body = build_search_body("contacts", ["email"], 123, 100, after="200")
    f = body["filterGroups"][0]["filters"][0]
    assert f == {"propertyName": "lastmodifieddate", "operator": "GTE", "value": "123"}
    assert body["sorts"] == [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}]
    assert "lastmodifieddate" in body["properties"] and body["after"] == "200"


def test_select_properties_discovers_and_skips_calculated():
    defs = [{"name": "dealname"}, {"name": "hs_date_entered_closedwon"}, {"name": "secret", "hidden": True}]
    assert select_properties(defs) == ["dealname"]
    assert select_properties(defs, ["amount", "dealname"]) == ["amount", "dealname"]


def test_flatten():
    rec = {"id": 7, "createdAt": "c", "updatedAt": "u", "archived": False,
           "properties": {"amount": 12.5, "dealname": "X", "empty": None}}
    row = flatten(rec, "sales", "deals")
    assert row["id"] == "7" and row["instance"] == "sales"
    assert row["p_amount"] == "12.5" and row["p_empty"] is None


def test_sample_config_loads():
    cfg = load_config(REPO / "config" / "ingest.yaml")
    assert [i.name for i in cfg.instances] == ["sales", "partners"]
    assert next(o for o in cfg.objects if o.name == "deals").associations == ("companies",)


def test_association_to_unknown_object_is_rejected(tmp_path):
    f = tmp_path / "c.yaml"
    f.write_text("instances: [{name: a, token_env: T}]\nobjects: [{name: deals, associations: [tickets]}]\n")
    with pytest.raises(ValueError, match="tickets"):
        load_config(f)


def test_missing_token_has_clear_error(monkeypatch):
    from crmingest.config import Instance
    monkeypatch.delenv("NOPE_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="NOPE_TOKEN"):
        Instance("a", "NOPE_TOKEN").token()

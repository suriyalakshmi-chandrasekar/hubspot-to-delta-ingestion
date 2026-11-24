"""Pure helpers: request bodies, timestamps, record flattening, property selection."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Contacts use a different "last modified" property from every other object.
MODIFIED_PROPERTY = {"contacts": "lastmodifieddate"}
DEFAULT_MODIFIED_PROPERTY = "hs_lastmodifieddate"

# Calculated or very wide properties that are expensive and rarely useful in bronze.
SKIP_PROPERTY_PREFIXES = ("hs_date_entered_", "hs_date_exited_", "hs_time_in_", "hs_v2_")


def modified_property(object_type: str) -> str:
    return MODIFIED_PROPERTY.get(object_type, DEFAULT_MODIFIED_PROPERTY)


def to_epoch_ms(value: str | int | float | datetime) -> int:
    """HubSpot returns ISO-8601 strings and accepts epoch milliseconds in filters."""
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def build_search_body(object_type: str, properties: list[str], since_ms: int, limit: int,
                      after: str | None = None) -> dict[str, Any]:
    prop = modified_property(object_type)
    body: dict[str, Any] = {
        "filterGroups": [{"filters": [{"propertyName": prop, "operator": "GTE", "value": str(since_ms)}]}],
        "sorts": [{"propertyName": prop, "direction": "ASCENDING"}],
        "properties": sorted(set(properties) | {prop}),
        "limit": limit,
    }
    if after:
        body["after"] = after
    return body


def select_properties(definitions: list[dict[str, Any]], include: list[str] | None = None,
                      exclude_prefixes: tuple[str, ...] = SKIP_PROPERTY_PREFIXES) -> list[str]:
    """Pick properties to extract.

    ``include`` given: use exactly those (plus nothing else).
    ``include`` empty: discover every property, minus noisy calculated ones.
    Property discovery means new custom fields appear in bronze automatically.
    """
    if include:
        return sorted(set(include))
    names = (d["name"] for d in definitions if not d.get("hidden", False))
    return sorted(n for n in names if not n.startswith(exclude_prefixes))


def flatten(record: dict[str, Any], instance: str, object_type: str) -> dict[str, Any]:
    """One HubSpot record -> one flat row. Property values stay strings in bronze."""
    row = {
        "instance": instance,
        "object_type": object_type,
        "id": str(record["id"]),
        "created_at": record.get("createdAt"),
        "updated_at": record.get("updatedAt") or record.get("properties", {}).get(modified_property(object_type)),
        "archived": bool(record.get("archived", False)),
        "archived_at": record.get("archivedAt"),
    }
    for name, value in (record.get("properties") or {}).items():
        row[f"p_{name}"] = None if value is None else str(value)
    return row

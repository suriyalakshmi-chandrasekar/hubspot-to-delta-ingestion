"""A small HubSpot CRM v3/v4 client built for bulk extraction.

What it handles for you:

* **Rate limits.** 429 responses are retried, honouring ``Retry-After`` when
  HubSpot sends it, otherwise exponential backoff with jitter. 5xx and
  connection errors are retried the same way. A client-side throttle keeps
  you under a requests-per-second budget so you hit 429 less often.
* **Paging.** Every list/search call follows ``paging.next.after`` until done.
* **The search API's 10,000-result cap.** ``search_modified_since`` slices
  the time range: when a query reaches the cap, it starts a new query from
  the last modified timestamp it has seen. Large backfills keep working.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests

from .extract import build_search_body, modified_property, to_epoch_ms

log = logging.getLogger(__name__)

SEARCH_RESULT_CAP = 10_000
RETRY_STATUSES = {429, 500, 502, 503, 504}


class HubSpotError(RuntimeError):
    pass


@dataclass
class Throttle:
    """Simple thread-safe limiter: at most ``rate`` calls per second."""

    rate: float
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _next_at: float = 0.0

    def wait(self) -> None:
        if self.rate <= 0:
            return
        with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            self._next_at = max(now, self._next_at) + 1.0 / self.rate
        if delay > 0:
            time.sleep(delay)


class HubSpotClient:
    def __init__(
        self,
        token: str,
        base_url: str = "https://api.hubapi.com",
        max_retries: int = 6,
        requests_per_second: float = 8.0,
        page_size: int = 100,
        timeout: float = 60.0,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.page_size = page_size
        self.timeout = timeout
        self.throttle = Throttle(requests_per_second)
        self._sleep = sleep
        self.session = session or requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        self.calls = 0

    # ------------------------------------------------------------------ http
    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        for attempt in range(1, self.max_retries + 2):
            self.throttle.wait()
            self.calls += 1
            try:
                resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
            except requests.ConnectionError as exc:
                if attempt > self.max_retries:
                    raise HubSpotError(f"{method} {path}: connection failed") from exc
                self._backoff(attempt, None, "connection error")
                continue

            if resp.status_code in RETRY_STATUSES and attempt <= self.max_retries:
                self._backoff(attempt, resp.headers.get("Retry-After"), f"HTTP {resp.status_code}")
                continue
            if resp.status_code >= 400:
                raise HubSpotError(f"{method} {path}: HTTP {resp.status_code} {resp.text[:300]}")
            return resp.json() if resp.content else {}
        raise HubSpotError(f"{method} {path}: gave up after {self.max_retries} retries")

    def _backoff(self, attempt: int, retry_after: str | None, reason: str) -> None:
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = 1.0
        else:
            delay = random.uniform(0, min(30.0, 0.5 * 2 ** (attempt - 1)))
        log.warning("%s, retry %d in %.2fs", reason, attempt, delay)
        self._sleep(delay)

    # ------------------------------------------------------------ endpoints
    def list_properties(self, object_type: str) -> list[dict[str, Any]]:
        """All property definitions for an object type (name, type, ...)."""
        return self._request("GET", f"/crm/v3/properties/{object_type}").get("results", [])

    def list_objects(self, object_type: str, properties: list[str], archived: bool = False) -> Iterator[dict]:
        """Every record of an object type (full load), or every archived record (deletes)."""
        after = None
        while True:
            params = {"limit": self.page_size, "archived": str(archived).lower(),
                      "properties": ",".join(properties)}
            if after:
                params["after"] = after
            page = self._request("GET", f"/crm/v3/objects/{object_type}", params=params)
            yield from page.get("results", [])
            after = page.get("paging", {}).get("next", {}).get("after")
            if not after:
                return

    def search_modified_since(self, object_type: str, properties: list[str], since_ms: int) -> Iterator[dict]:
        """Records modified at or after ``since_ms``, oldest first, past the 10k cap."""
        prop = modified_property(object_type)
        lower = since_ms
        while True:
            seen_in_window = 0
            last_modified = None
            after = None
            while True:
                body = build_search_body(object_type, properties, lower, self.page_size, after)
                page = self._request("POST", f"/crm/v3/objects/{object_type}/search", json=body)
                for record in page.get("results", []):
                    seen_in_window += 1
                    last_modified = record["properties"].get(prop) or last_modified
                    yield record
                after = page.get("paging", {}).get("next", {}).get("after")
                if not after:
                    return                       # window exhausted: done
                if seen_in_window + self.page_size > SEARCH_RESULT_CAP:
                    break                        # next page would pass the cap: slice
            if last_modified is None:
                return
            new_lower = to_epoch_ms(last_modified)
            if new_lower <= lower:
                # more than 10k records share one timestamp: step past it rather than loop forever
                log.warning("%s: >%d records at %s; stepping 1 ms forward",
                            object_type, SEARCH_RESULT_CAP, last_modified)
                new_lower = lower + 1
            log.info("%s: hit the search cap, continuing from %s", object_type, last_modified)
            lower = new_lower                    # >= boundary: duplicates are removed by the MERGE

    def batch_associations(self, from_type: str, to_type: str, ids: list[str], batch_size: int = 100) -> Iterator[dict]:
        """v4 associations for many records: yields {from_id, to_id, type_id, category, label}."""
        for i in range(0, len(ids), batch_size):
            body = {"inputs": [{"id": x} for x in ids[i:i + batch_size]]}
            page = self._request("POST", f"/crm/v4/associations/{from_type}/{to_type}/batch/read", json=body)
            for result in page.get("results", []):
                from_id = str(result["from"]["id"])
                for to in result.get("to", []):
                    for t in to.get("associationTypes", []) or [{}]:
                        yield {"from_id": from_id, "to_id": str(to["toObjectId"]),
                               "type_id": t.get("typeId"), "category": t.get("category"), "label": t.get("label")}

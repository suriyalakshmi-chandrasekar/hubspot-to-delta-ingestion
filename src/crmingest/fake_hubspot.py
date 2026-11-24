"""A fake HubSpot CRM API for demos and tests. Standard library only.

It implements the endpoints the client uses, with the behaviour that makes
real ingestion tricky:

* paging with ``after`` cursors
* the search API's 10,000-result cap (an ``after`` past it returns HTTP 400)
* random 429 rate-limit responses with ``Retry-After``
* archived (deleted) records
* v4 batch associations

    python -m crmingest.fake_hubspot --port 8765

All data is synthetic and generated from a seed.
"""

from __future__ import annotations

import argparse
import json
import random
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOKEN = "test-token"
SEARCH_CAP = 10_000
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
INDUSTRIES = ["LOGISTICS", "RETAIL", "MANUFACTURING", "FOOD_AND_BEVERAGE", "AUTOMOTIVE"]
STAGES = ["appointmentscheduled", "qualifiedtobuy", "contractsent", "closedwon", "closedlost"]
PROPS = {
    "companies": ["name", "domain", "industry", "city", "hs_lastmodifieddate"],
    "contacts": ["firstname", "lastname", "email", "jobtitle", "lastmodifieddate"],
    "deals": ["dealname", "amount", "dealstage", "closedate", "hs_lastmodifieddate"],
}
MODIFIED = {"contacts": "lastmodifieddate"}


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Store:
    """In-memory CRM data with a 'next day' mutation for incremental demos."""

    def __init__(self, companies=300, contacts=900, deals=600, seed=7, rate_limit_every=0):
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.rate_limit_every = rate_limit_every
        self.requests = 0
        self.objects: dict[str, dict[str, dict]] = {"companies": {}, "contacts": {}, "deals": {}}
        self.archived: dict[str, dict[str, dict]] = {"companies": {}, "contacts": {}, "deals": {}}
        self.assoc: dict[tuple[str, str], dict[str, set[str]]] = {}
        self.clock = START
        for i in range(1, companies + 1):
            self._put("companies", str(i), self._company(i))
        for i in range(1, contacts + 1):
            self._put("contacts", str(10_000 + i), self._contact(i))
            self._link("contacts", str(10_000 + i), "companies", str(self.rng.randint(1, companies)))
        for i in range(1, deals + 1):
            self._put("deals", str(50_000 + i), self._deal(i))
            self._link("deals", str(50_000 + i), "companies", str(self.rng.randint(1, companies)))

    # ---- data
    def _tick(self) -> datetime:
        self.clock += timedelta(seconds=self.rng.randint(1, 30))
        return self.clock

    def _company(self, i):
        return {"name": f"Company {i:04d}", "domain": f"company{i}.example.com",
                "industry": self.rng.choice(INDUSTRIES), "city": self.rng.choice(["Austin", "Denver", "Atlanta"])}

    def _contact(self, i):
        return {"firstname": f"First{i}", "lastname": f"Last{i}", "email": f"person{i}@example.com",
                "jobtitle": self.rng.choice(["Buyer", "Logistics Manager", "VP Supply Chain", None])}

    def _deal(self, i):
        return {"dealname": f"Deal {i:05d}", "amount": str(round(self.rng.uniform(1_000, 90_000), 2)),
                "dealstage": self.rng.choice(STAGES), "closedate": iso(START + timedelta(days=i % 90))}

    def _put(self, obj, oid, props):
        ts = iso(self._tick())
        existing = self.objects[obj].get(oid)
        created = existing["createdAt"] if existing else ts
        props = {**(existing["properties"] if existing else {}), **props, MODIFIED.get(obj, "hs_lastmodifieddate"): ts}
        self.objects[obj][oid] = {"id": oid, "properties": props, "createdAt": created,
                                  "updatedAt": ts, "archived": False}

    def _link(self, a, aid, b, bid):
        self.assoc.setdefault((a, b), {}).setdefault(aid, set()).add(bid)
        self.assoc.setdefault((b, a), {}).setdefault(bid, set()).add(aid)

    def next_day(self, updates=40, new=25, deletes=5):
        """Modify, add and archive some records (simulates a day of CRM activity)."""
        with self.lock:
            self.clock += timedelta(days=1)
            for oid in self.rng.sample(sorted(self.objects["deals"]), updates):
                self._put("deals", oid, {"dealstage": "closedwon"})
            start = 50_000 + len(self.objects["deals"]) + len(self.archived["deals"]) + 1
            for i in range(start, start + new):
                self._put("deals", str(i), self._deal(i))
                self._link("deals", str(i), "companies", "1")
            for oid in self.rng.sample(sorted(self.objects["contacts"]), deletes):
                rec = self.objects["contacts"].pop(oid)
                self.archived["contacts"][oid] = {**rec, "archived": True, "archivedAt": iso(self._tick())}

    # ---- queries
    def page(self, items, after, limit):
        start = int(after or 0)
        chunk = items[start:start + limit]
        nxt = start + limit
        paging = {"next": {"after": str(nxt)}} if nxt < len(items) else {}
        return {"results": chunk, "paging": paging} if paging else {"results": chunk}


class Handler(BaseHTTPRequestHandler):
    store: Store

    def log_message(self, *args):  # quiet
        pass

    def _send(self, status, body=None, headers=None):
        payload = json.dumps(body or {}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _guard(self) -> bool:
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._send(401, {"message": "invalid token"})
            return False
        s = self.store
        with s.lock:
            s.requests += 1
            limited = s.rate_limit_every and s.requests % s.rate_limit_every == 0
        if limited:
            self._send(429, {"status": "error", "message": "rate limit"}, {"Retry-After": "0"})
            return False
        return True

    def do_GET(self):  # noqa: N802
        if not self._guard():
            return
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        parts = url.path.strip("/").split("/")
        s = self.store
        if parts[:3] == ["crm", "v3", "properties"]:
            obj = parts[3]
            return self._send(200, {"results": [{"name": p, "type": "string"} for p in PROPS.get(obj, [])]
                                    + [{"name": "hs_date_entered_closedwon", "type": "datetime"}]})
        if parts[:3] == ["crm", "v3", "objects"] and len(parts) == 4:
            obj = parts[3]
            source = s.archived if q.get("archived") == "true" else s.objects
            items = sorted(source.get(obj, {}).values(), key=lambda r: int(r["id"]))
            wanted = [p for p in q.get("properties", "").split(",") if p]
            items = [self._project(r, wanted) for r in items]
            return self._send(200, s.page(items, q.get("after"), min(int(q.get("limit", 100)), 100)))
        self._send(404, {"message": "not found"})

    def do_POST(self):  # noqa: N802
        if not self._guard():
            return
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        parts = urlparse(self.path).path.strip("/").split("/")
        s = self.store
        if parts[:3] == ["crm", "v3", "objects"] and parts[-1] == "search":
            obj = parts[3]
            f = body["filterGroups"][0]["filters"][0]
            since = int(f["value"])
            prop = f["propertyName"]
            def ms(r):
                return int(datetime.fromisoformat(r["properties"][prop].replace("Z", "+00:00")).timestamp() * 1000)
            items = sorted((r for r in s.objects[obj].values() if ms(r) >= since), key=ms)
            after = int(body.get("after") or 0)
            if after >= SEARCH_CAP:
                return self._send(400, {"message": "search results are limited to 10,000"})
            limit = min(int(body.get("limit", 100)), 200)
            items = [self._project(r, body.get("properties", [])) for r in items]
            page = s.page(items, after, limit)
            page["total"] = len(items)
            return self._send(200, page)
        if parts[:3] == ["crm", "v4", "associations"] and parts[-2:] == ["batch", "read"]:
            a, b = parts[3], parts[4]
            links = s.assoc.get((a, b), {})
            kind = [{"category": "HUBSPOT_DEFINED", "typeId": 1, "label": None}]
            results = [{"from": {"id": i["id"]},
                        "to": [{"toObjectId": int(t), "associationTypes": kind} for t in sorted(links[i["id"]])]}
                       for i in body.get("inputs", []) if i["id"] in links]
            return self._send(200, {"status": "COMPLETE", "results": results})
        if parts == ["_admin", "next-day"]:
            s.next_day()
            return self._send(200, {"ok": True})
        self._send(404, {"message": "not found"})

    @staticmethod
    def _project(record, wanted):
        if not wanted:
            return record
        return {**record, "properties": {k: v for k, v in record["properties"].items() if k in wanted}}


def serve(port: int = 0, **store_kwargs) -> tuple[ThreadingHTTPServer, str]:
    """Start the fake API in a background thread. Returns (server, base_url)."""
    handler = type("BoundHandler", (Handler,), {"store": Store(**store_kwargs)})
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--rate-limit-every", type=int, default=25)
    args = ap.parse_args()
    srv, url = serve(args.port, rate_limit_every=args.rate_limit_every)
    print(f"fake HubSpot API on {url} (token: {TOKEN}); POST {url}/_admin/next-day to simulate a day")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        srv.shutdown()

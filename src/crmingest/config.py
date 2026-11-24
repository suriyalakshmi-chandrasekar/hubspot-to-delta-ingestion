"""Which CRM instances and objects to ingest, read from YAML."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Instance:
    name: str                      # short label stored in every row, e.g. "sales"
    token_env: str                 # env var (or secret key) holding the private-app token
    base_url: str = "https://api.hubapi.com"

    def token(self) -> str:
        value = os.environ.get(self.token_env)
        if not value:
            raise RuntimeError(f"instance '{self.name}': set the {self.token_env} environment variable")
        return value


@dataclass(frozen=True)
class ObjectConfig:
    name: str                                   # companies | contacts | deals | notes | ...
    properties: tuple[str, ...] = ()            # empty = discover all properties
    track_deletes: bool = True                  # soft-delete records archived in HubSpot
    associations: tuple[str, ...] = ()          # object types to pull associations to


@dataclass(frozen=True)
class IngestConfig:
    instances: tuple[Instance, ...]
    objects: tuple[ObjectConfig, ...]
    target_schema: str = "bronze"
    overlap_minutes: int = 5                    # re-read a small overlap; MERGE removes duplicates
    max_workers: int = 4
    requests_per_second: float = 8.0
    chunk_size: int = 5_000
    extra: dict = field(default_factory=dict)


def load_config(path: str | Path) -> IngestConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    instances = tuple(Instance(**i) for i in raw["instances"])
    objects = tuple(
        ObjectConfig(name=o["name"], properties=tuple(o.get("properties", ())),
                     track_deletes=o.get("track_deletes", True),
                     associations=tuple(o.get("associations", ())))
        for o in raw["objects"]
    )
    if len({i.name for i in instances}) != len(instances):
        raise ValueError("instance names must be unique")
    known = {o.name for o in objects}
    for o in objects:
        unknown = [a for a in o.associations if a not in known]
        if unknown:
            raise ValueError(f"{o.name}: associations to objects not configured: {unknown}")
    settings = {k: v for k, v in raw.items() if k not in ("instances", "objects")}
    return IngestConfig(instances=instances, objects=objects, **settings)

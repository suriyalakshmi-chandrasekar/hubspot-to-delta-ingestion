"""Land CRM objects and associations in Delta bronze tables.

Per (instance, object):

1. discover properties (new custom fields show up automatically)
2. extract: full list on the first run, then search "modified since watermark"
3. write in chunks with MERGE on (instance, id); an older copy never overwrites a newer one
4. soft-delete records that HubSpot reports as archived
5. refresh associations for the records that changed
6. move the watermark forward

(instance, object) pairs run in parallel threads.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from .client import HubSpotClient
from .config import IngestConfig, Instance, ObjectConfig
from .extract import flatten, select_properties, to_epoch_ms

log = logging.getLogger("crmingest")

BASE_FIELDS = [
    StructField("instance", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("id", StringType(), False),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("archived", BooleanType()),
    StructField("archived_at", StringType()),
]
BASE_NAMES = {f.name for f in BASE_FIELDS}


@dataclass
class ObjectResult:
    instance: str
    object_type: str
    mode: str
    upserted: int = 0
    soft_deleted: int = 0
    associations: int = 0
    api_calls: int = 0
    seconds: float = 0.0
    error: str | None = None


def chunks(it: Iterable, size: int) -> Iterator[list]:
    it = iter(it)
    while batch := list(islice(it, size)):
        yield batch


class Ingestor:
    def __init__(self, spark: SparkSession, config: IngestConfig,
                 client_factory: Callable[[Instance], HubSpotClient] | None = None):
        self.spark = spark
        self.cfg = config
        self.schema = config.target_schema
        # API extraction runs in parallel; writes to the same Delta table are serialized
        # so two instances never race to create or MERGE one table.
        self._locks: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
        self.client_factory = client_factory or (
            lambda inst: HubSpotClient(inst.token(), inst.base_url,
                                       requests_per_second=config.requests_per_second))
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # new properties -> new columns
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.schema}._crm_watermarks
                      (instance STRING, object_type STRING, watermark_ms BIGINT, updated_at TIMESTAMP) USING DELTA""")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.schema}.crm_associations
                      (instance STRING, from_type STRING, from_id STRING, to_type STRING, to_id STRING,
                       type_id INT, category STRING, label STRING, loaded_at TIMESTAMP) USING DELTA""")

    # ------------------------------------------------------------ public
    def run(self, full_refresh: bool = False) -> list[ObjectResult]:
        jobs = [(i, o) for i in self.cfg.instances for o in self.cfg.objects]
        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as pool:
            return list(pool.map(lambda job: self.ingest(*job, full_refresh=full_refresh), jobs))

    def ingest(self, inst: Instance, obj: ObjectConfig, full_refresh: bool = False) -> ObjectResult:
        started = time.monotonic()
        client = self.client_factory(inst)
        watermark = None if full_refresh else self._get_watermark(inst.name, obj.name)
        result = ObjectResult(inst.name, obj.name, "full" if watermark is None else "incremental")
        try:
            props = select_properties(client.list_properties(obj.name), list(obj.properties))
            if watermark is None:
                records = client.list_objects(obj.name, props)
            else:
                since = watermark - self.cfg.overlap_minutes * 60_000
                records = client.search_modified_since(obj.name, props, since)

            changed_ids: list[str] = []
            max_seen = watermark
            for batch in chunks(records, self.cfg.chunk_size):
                rows = [flatten(r, inst.name, obj.name) for r in batch]
                with self._locks[self._table(obj.name)]:
                    self._merge_records(obj.name, rows)
                result.upserted += len(rows)
                changed_ids.extend(r["id"] for r in rows)
                newest = max(to_epoch_ms(r["updated_at"]) for r in rows if r["updated_at"])
                max_seen = newest if max_seen is None else max(max_seen, newest)

            if obj.track_deletes:
                archived = [flatten(r, inst.name, obj.name) for r in client.list_objects(obj.name, [], archived=True)]
                with self._locks[self._table(obj.name)]:
                    result.soft_deleted = self._soft_delete(obj.name, archived)

            for to_type in obj.associations:
                links = list(client.batch_associations(obj.name, to_type, changed_ids)) if changed_ids else []
                with self._locks["associations"]:
                    result.associations += self._refresh_associations(inst.name, obj.name, to_type, changed_ids, links)

            if max_seen is not None:
                with self._locks["watermarks"]:
                    self._set_watermark(inst.name, obj.name, max_seen)
        except Exception as exc:  # recorded per object; other objects keep running
            log.exception("%s/%s failed", inst.name, obj.name)
            result.error = repr(exc)
        result.api_calls = client.calls
        result.seconds = round(time.monotonic() - started, 1)
        log.info("%-8s %-10s %-11s upserted=%-6d deleted=%-4d assoc=%-6d calls=%-4d %ss %s",
                 inst.name, obj.name, result.mode, result.upserted, result.soft_deleted,
                 result.associations, result.api_calls, result.seconds, result.error or "")
        return result

    # ------------------------------------------------------------ writes
    def _table(self, object_type: str) -> str:
        return f"{self.schema}.crm_{object_type}"

    def _frame(self, rows: list[dict]) -> DataFrame:
        prop_cols = sorted({k for r in rows for k in r} - BASE_NAMES)
        schema = StructType(BASE_FIELDS + [StructField(c, StringType()) for c in prop_cols])
        data = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
        return (self.spark.createDataFrame(data, schema)
                .withColumn("_loaded_at", F.current_timestamp()))

    def _merge_records(self, object_type: str, rows: list[dict]) -> None:
        df = self._frame(rows)
        # keep the newest copy per key inside the chunk (search overlap can repeat a record)
        from pyspark.sql.window import Window
        newest = Window.partitionBy("instance", "id").orderBy(F.col("updated_at").desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(newest)).where("_rn = 1").drop("_rn")
        table = self._table(object_type)
        if not self.spark.catalog.tableExists(table):
            df.write.format("delta").saveAsTable(table)
            return
        from delta.tables import DeltaTable
        (DeltaTable.forName(self.spark, table).alias("t")
         .merge(df.alias("s"), "t.instance = s.instance AND t.id = s.id")
         .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at OR t.updated_at IS NULL")
         .whenNotMatchedInsertAll()
         .execute())

    def _soft_delete(self, object_type: str, archived_rows: list[dict]) -> int:
        table = self._table(object_type)
        if not archived_rows or not self.spark.catalog.tableExists(table):
            return 0
        df = self._frame(archived_rows).select("instance", "id", "archived_at").dropDuplicates(["instance", "id"])
        from delta.tables import DeltaTable
        before = self.spark.table(table).where("archived").count()
        (DeltaTable.forName(self.spark, table).alias("t")
         .merge(df.alias("s"), "t.instance = s.instance AND t.id = s.id")
         .whenMatchedUpdate(condition="NOT t.archived",
                            set={"archived": "true", "archived_at": "s.archived_at",
                                 "_loaded_at": "current_timestamp()"})
         .execute())
        return self.spark.table(table).where("archived").count() - before

    def _refresh_associations(self, instance: str, from_type: str, to_type: str,
                              ids: list[str], links: list[dict]) -> int:
        """Replace associations for the changed records, so removed links disappear too."""
        if not ids:
            return 0
        table = f"{self.schema}.crm_associations"
        ids_df = self.spark.createDataFrame([(i,) for i in set(ids)], "from_id STRING")
        ids_df.createOrReplaceTempView(view := f"_assoc_ids_{from_type}_{to_type}_{instance}")
        self.spark.sql(f"""DELETE FROM {table} WHERE instance = '{instance}' AND from_type = '{from_type}'
                           AND to_type = '{to_type}' AND from_id IN (SELECT from_id FROM {view})""")
        self.spark.catalog.dropTempView(view)
        if links:
            rows = [(instance, from_type, l["from_id"], to_type, l["to_id"], l["type_id"], l["category"], l["label"])
                    for l in links]
            (self.spark.createDataFrame(rows, "instance STRING, from_type STRING, from_id STRING, to_type STRING, "
                                              "to_id STRING, type_id INT, category STRING, label STRING")
             .withColumn("loaded_at", F.current_timestamp())
             .write.format("delta").mode("append").saveAsTable(table))
        return len(links)

    # ------------------------------------------------------------ state
    def _get_watermark(self, instance: str, object_type: str) -> int | None:
        row = self.spark.sql(f"""SELECT watermark_ms FROM {self.schema}._crm_watermarks
                                 WHERE instance = '{instance}' AND object_type = '{object_type}'""").first()
        return None if row is None else int(row.watermark_ms)

    def _set_watermark(self, instance: str, object_type: str, watermark_ms: int) -> None:
        self.spark.sql(f"""
            MERGE INTO {self.schema}._crm_watermarks t
            USING (SELECT '{instance}' AS instance, '{object_type}' AS object_type,
                          CAST({watermark_ms} AS BIGINT) AS watermark_ms) s
            ON t.instance = s.instance AND t.object_type = s.object_type
            WHEN MATCHED AND s.watermark_ms > t.watermark_ms
              THEN UPDATE SET watermark_ms = s.watermark_ms, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (instance, object_type, watermark_ms, updated_at)
              VALUES (s.instance, s.object_type, s.watermark_ms, current_timestamp())
        """)

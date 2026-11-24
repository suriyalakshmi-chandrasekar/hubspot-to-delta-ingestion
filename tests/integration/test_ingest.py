"""End-to-end: fake HubSpot API -> local Spark + Delta bronze tables. Runs in CI (Linux)."""

import os
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    from crmingest.client import HubSpotClient
    from crmingest.config import load_config
    from crmingest.fake_hubspot import TOKEN, serve
    from crmingest.pipeline import Ingestor

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        wh = tmp_path_factory.mktemp("wh")
        builder = (SparkSession.builder.master("local[2]").appName("crm-tests")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   .config("spark.sql.warehouse.dir", str(wh))
                   .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false"))
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Spark not available here: {exc}")

    server, url = serve(rate_limit_every=25)
    cfg = load_config(REPO / "config" / "ingest.yaml")
    cfg = replace(cfg, instances=tuple(replace(i, base_url=url) for i in cfg.instances), chunk_size=400)
    for i in cfg.instances:
        os.environ[i.token_env] = TOKEN
    ingestor = Ingestor(spark, cfg, lambda inst: HubSpotClient(inst.token(), inst.base_url,
                                                               requests_per_second=0, sleep=lambda s: None))
    yield spark, ingestor, server
    server.shutdown()


def test_day1_full_load(env):
    spark, ingestor, _ = env
    results = ingestor.run()
    assert all(r.error is None for r in results), [r.error for r in results]
    assert {r.mode for r in results} == {"full"}
    # two instances read the same fake API -> 2x rows, kept apart by the instance column
    assert spark.table("bronze.crm_companies").count() == 600
    assert spark.table("bronze.crm_contacts").count() == 1800
    assert spark.table("bronze.crm_deals").count() == 1200
    # discovered properties became columns; calculated ones were skipped
    cols = set(spark.table("bronze.crm_companies").columns)
    assert {"p_name", "p_domain", "p_industry"} <= cols and "p_hs_date_entered_closedwon" not in cols
    # deals were configured with an explicit property list
    assert "p_dealstage" in spark.table("bronze.crm_deals").columns
    assert spark.table("bronze.crm_associations").count() > 0


def test_day2_incremental_deletes_and_associations(env):
    spark, ingestor, server = env
    server.RequestHandlerClass.store.next_day(updates=40, new=25, deletes=5)
    results = {(r.instance, r.object_type): r for r in ingestor.run()}

    deals = results[("sales", "deals")]
    assert deals.mode == "incremental"
    assert 65 <= deals.upserted < 200                     # changes + overlap, not the full 600
    assert spark.table("bronze.crm_deals").count() == 1250  # 25 new per instance
    assert spark.table("bronze.crm_deals").where("p_dealstage = 'closedwon'").count() >= 80

    contacts = spark.table("bronze.crm_contacts")
    assert contacts.count() == 1800                       # soft delete: rows kept
    assert contacts.where("archived").count() == 10       # 5 per instance
    assert results[("sales", "contacts")].soft_deleted == 5

    # new deals got their company links
    new_links = spark.sql("SELECT count(*) c FROM bronze.crm_associations WHERE from_type='deals' "
                          "AND instance='sales' AND to_id='1'").first().c
    assert new_links >= 25


def test_rerun_with_no_changes_reads_only_the_overlap(env):
    spark, ingestor, _ = env
    before = spark.table("bronze.crm_deals").count()
    results = ingestor.run()
    assert all(r.error is None for r in results)
    assert spark.table("bronze.crm_deals").count() == before
    assert spark.table("bronze._crm_watermarks").count() == 6


def test_watermark_never_moves_backwards(env):
    spark, ingestor, _ = env
    before = {(r.instance, r.object_type): r.watermark_ms for r in spark.table("bronze._crm_watermarks").collect()}
    ingestor._set_watermark("sales", "deals", 1)
    after = spark.sql("SELECT watermark_ms FROM bronze._crm_watermarks "
                      "WHERE instance='sales' AND object_type='deals'").first().watermark_ms
    assert after == before[("sales", "deals")]

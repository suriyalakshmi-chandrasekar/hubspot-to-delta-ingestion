"""End-to-end demo against the fake HubSpot API (no HubSpot account needed).

    python examples/demo.py

Runs on local Spark + Delta (Linux/macOS, or Windows with HADOOP_HOME set).
Day 1 is a full load; day 2 picks up only changed records, new records,
archived (deleted) contacts and refreshed associations.
"""

import logging
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crmingest.client import HubSpotClient
from crmingest.config import load_config
from crmingest.fake_hubspot import TOKEN, serve
from crmingest.pipeline import Ingestor

logging.basicConfig(level=logging.INFO, format="%(message)s")
for noisy in ("py4j", "urllib3", "crmingest.client"):
    logging.getLogger(noisy).setLevel(logging.ERROR)


def main():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    shutil.rmtree(ROOT / "spark-warehouse", ignore_errors=True)
    builder = (SparkSession.builder.master("local[*]").appName("crm-demo")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    server, url = serve(rate_limit_every=40)          # every 40th call returns 429
    cfg = load_config(ROOT / "config" / "ingest.yaml")
    cfg = replace(cfg, instances=tuple(replace(i, base_url=url) for i in cfg.instances))
    for i in cfg.instances:
        os.environ[i.token_env] = TOKEN
    ingestor = Ingestor(spark, cfg, lambda inst: HubSpotClient(inst.token(), inst.base_url,
                                                               requests_per_second=0, sleep=lambda s: None))

    print("\n=== DAY 1: full load ===")
    ingestor.run()
    print("\n=== DAY 2: incremental ===")
    requests.post(f"{url}/_admin/next-day", headers={"Authorization": f"Bearer {TOKEN}"})
    ingestor.run()

    spark.sql("SELECT instance, dealstage AS stage, count(*) AS deals FROM "
              "(SELECT instance, p_dealstage AS dealstage FROM bronze.crm_deals) GROUP BY 1, 2 ORDER BY 1, 2").show()
    spark.sql("SELECT instance, count_if(archived) AS archived_contacts, count(*) AS contacts "
              "FROM bronze.crm_contacts GROUP BY instance").show()
    spark.sql("SELECT from_type, to_type, count(*) AS links FROM bronze.crm_associations GROUP BY 1, 2").show()
    spark.table("bronze._crm_watermarks").show(truncate=False)
    server.shutdown()


if __name__ == "__main__":
    main()

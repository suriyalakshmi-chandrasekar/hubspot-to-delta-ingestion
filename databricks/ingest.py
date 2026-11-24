# Databricks notebook source
# MAGIC %md
# MAGIC # HubSpot to bronze
# MAGIC Tokens come from a secret scope (one key per instance, named like the `token_env` in `config/ingest.yaml`).

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath("..")
sys.path.insert(0, os.path.join(repo_root, "src"))
dbutils.widgets.text("secret_scope", "crm")  # noqa: F821
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])  # noqa: F821
scope = dbutils.widgets.get("secret_scope")  # noqa: F821

# COMMAND ----------

import logging

from crmingest.config import load_config
from crmingest.pipeline import Ingestor

logging.basicConfig(level=logging.INFO, format="%(message)s")
cfg = load_config(os.path.join(repo_root, "config", "ingest.yaml"))
for inst in cfg.instances:  # expose secrets to the client as env vars for this run only
    os.environ[inst.token_env] = dbutils.secrets.get(scope, inst.token_env)  # noqa: F821

results = Ingestor(spark, cfg).run(full_refresh=dbutils.widgets.get("full_refresh") == "true")  # noqa: F821
display(spark.createDataFrame([r.__dict__ for r in results]))  # noqa: F821

failed = [f"{r.instance}/{r.object_type}" for r in results if r.error]
if failed:
    raise RuntimeError(f"failed: {failed}")

"""
src/utils/config.py
Centralized configuration loader for Databricks pipeline.
"""
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import SparkSession


def load_config(config_path: str = "03_config/pipeline_config.yaml") -> Dict[str, Any]:
    base_path = Path(__file__).resolve().parents[3]
    full_path = base_path / config_path
    with open(full_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_spark_conf(spark: SparkSession, key: str, default: Any = None) -> Any:
    return spark.conf.get(key, default)


def resolve_delta_path(spark: SparkSession, layer: str, config: Dict[str, Any]) -> str:
    layer_cfg = config["layers"][layer]
    if spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true":
        return f"{config['unity_catalog']['catalog_name']}.{config['unity_catalog']['schema_name']}.{layer_cfg['table']}"
    return layer_cfg["path"]


def get_landing_path(config: Dict[str, Any]) -> str:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return config["paths"]["landing_raw"]
    return "landing/"

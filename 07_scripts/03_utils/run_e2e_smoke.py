"""Quick end-to-end smoke test of the medallion pipeline."""
import sys
sys.path.insert(0, ".")

from src.medenroll834.common.spark_session import get_local_spark
from src.medenroll834.ingestion.pipeline import run_pipeline


def main():
    spark = get_local_spark("e2e-test")
    try:
        summary = run_pipeline(
            spark,
            landing_path="10_data/raw/",
            config_path="03_config/pipeline_config.yaml",
        )
        print("PIPELINE_SUMMARY_START")
        for k, v in summary.items():
            print(f"{k}={v}")
        print("PIPELINE_SUMMARY_END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

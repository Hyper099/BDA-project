"""PySpark ingestion, cleaning, and feature engineering pipeline."""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from utils.config import PROCESSED_DATA_PATH, RAW_DATA_PATH, S3_PROCESSED_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


def build_spark(app_name: str = "FinancialInclusionETL") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_pipeline(input_csv: Path = RAW_DATA_PATH, output_csv: Path = PROCESSED_DATA_PATH) -> str:
    spark = build_spark()
    logger.info("Reading raw dataset from %s", input_csv)
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(input_csv))

    numeric_cols = [
        "monthly_upi_transactions",
        "avg_transaction_amount",
        "bill_payment_timeliness",
        "mobile_recharge_frequency",
        "age",
        "income",
        "savings_ratio",
        "spending_ratio",
        "payment_delay_days",
    ]

    for c in numeric_cols:
        median_val = df.approxQuantile(c, [0.5], 0.01)[0]
        df = df.fillna({c: median_val})

    df = df.fillna({"location_type": "rural", "occupation": "gig_worker"})

    # Business-friendly engineered indicators for downstream models.
    df = (
        df.withColumn("digital_activity_score", col("monthly_upi_transactions") * col("avg_transaction_amount") / 1000.0)
        .withColumn("financial_discipline_score", (col("bill_payment_timeliness") + col("savings_ratio")) / 2.0)
        .withColumn("is_underbanked", when(col("monthly_upi_transactions") < 8, 1).otherwise(0))
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = output_csv.parent / "spark_output_tmp"

    logger.info("Writing processed data to %s", output_csv)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(temp_dir))
    )

    part_file = next(temp_dir.glob("part-*.csv"))
    part_file.replace(output_csv)

    for file in temp_dir.glob("*"):
        if file.exists():
            file.unlink(missing_ok=True)
    temp_dir.rmdir()

    S3_PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(output_csv, S3_PROCESSED_PATH)

    spark.stop()
    logger.info("ETL pipeline complete. Output saved at %s", output_csv)
    return str(output_csv)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PySpark ETL pipeline")
    parser.add_argument("--input", type=str, default=str(RAW_DATA_PATH), help="Raw CSV path")
    parser.add_argument("--output", type=str, default=str(PROCESSED_DATA_PATH), help="Processed CSV path")
    args = parser.parse_args()

    run_pipeline(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()

"""Observable PySpark pipeline with explicit actions for Spark UI education."""
from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.config import DATA_DIR, RAW_DATA_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_local_ui_url(spark: SparkSession, ui_base_url: str | None) -> str:
    """Return a browser-friendly Spark UI base URL for local Spark sessions."""
    if not ui_base_url:
        return "http://localhost:4040"

    parsed = urlparse(ui_base_url)
    master = spark.sparkContext.master.lower()
    if master.startswith("local") and parsed.hostname and parsed.hostname not in {"localhost", "127.0.0.1"}:
        # Local Spark can report machine hostname; force localhost for reliable local browser access.
        netloc = f"localhost:{parsed.port}" if parsed.port else "localhost"
        return urlunparse((parsed.scheme or "http", netloc, parsed.path or "", "", "", ""))
    return ui_base_url


def build_spark(app_name: str = "ObservableFinancialInclusionPipeline") -> SparkSession:
    """Create Spark session configured for local development and UI visibility."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_data(spark: SparkSession, input_csv: Path) -> DataFrame:
    """Load CSV with schema inference."""
    logger.info("Loading raw data from %s", input_csv)
    return spark.read.option("header", True).option("inferSchema", True).csv(str(input_csv))


def transform_data(df: DataFrame) -> DataFrame:
    """Apply cleaning and feature engineering transformations."""
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

    transformed = df
    for col_name in numeric_cols:
        median_val = transformed.approxQuantile(col_name, [0.5], 0.01)[0]
        transformed = transformed.fillna({col_name: median_val})

    transformed = transformed.fillna({"location_type": "rural", "occupation": "gig_worker"})

    transformed = (
        transformed.withColumn(
            "digital_activity_score",
            F.col("monthly_upi_transactions") * F.col("avg_transaction_amount") / F.lit(1000.0),
        )
        .withColumn(
            "financial_discipline_score",
            (F.col("bill_payment_timeliness") + F.col("savings_ratio")) / F.lit(2.0),
        )
        .withColumn(
            "repayment_signal",
            (
                F.lit(0.035) * F.col("monthly_upi_transactions")
                + F.lit(0.00001) * F.col("income")
                + F.lit(1.35) * F.col("bill_payment_timeliness")
                + F.lit(1.1) * F.col("savings_ratio")
                - F.lit(0.85) * F.col("spending_ratio")
                - F.lit(0.04) * F.col("payment_delay_days")
            ),
        )
        .withColumn("is_underbanked", F.when(F.col("monthly_upi_transactions") < 8, 1).otherwise(0))
    )

    return transformed


def aggregate_metrics(df: DataFrame) -> DataFrame:
    """Aggregate metrics by location and occupation, then join back for richer DAG."""
    by_location = (
        df.groupBy("location_type")
        .agg(
            F.count("*").alias("location_population"),
            F.avg("income").alias("location_avg_income"),
            F.avg("repayment_signal").alias("location_avg_signal"),
        )
    )

    by_occupation = (
        df.groupBy("occupation")
        .agg(
            F.count("*").alias("occupation_population"),
            F.avg("savings_ratio").alias("occupation_avg_savings"),
            F.avg("payment_delay_days").alias("occupation_avg_delay"),
        )
    )

    return (
        df.join(by_location, on="location_type", how="left")
        .join(by_occupation, on="occupation", how="left")
        .withColumn("relative_income", F.col("income") / F.col("location_avg_income"))
        .withColumn("relative_savings", F.col("savings_ratio") / F.col("occupation_avg_savings"))
    )


def build_top_segments(df: DataFrame) -> DataFrame:
    """Rank top repayment-signal users in each location for an additional shuffle stage."""
    rank_window = Window.partitionBy("location_type").orderBy(F.col("repayment_signal").desc())
    return (
        df.withColumn("segment_rank", F.row_number().over(rank_window))
        .filter(F.col("segment_rank") <= 5)
        .orderBy("location_type", "segment_rank")
    )


def materialize_checkpoints(raw_df: DataFrame, transformed_df: DataFrame, enriched_df: DataFrame) -> dict:
    """Run stage-boundary actions so Spark UI shows clear job progression."""
    raw_count = raw_df.count()
    logger.info("Checkpoint action: loaded row count = %d", raw_count)

    transformed_count = transformed_df.count()
    logger.info("Checkpoint action: transformed row count = %d", transformed_count)

    summary = (
        enriched_df.groupBy("location_type")
        .agg(
            F.count("*").alias("rows"),
            F.avg("repayment_signal").alias("avg_repayment_signal"),
            F.avg("relative_income").alias("avg_relative_income"),
        )
        .orderBy("location_type")
    )
    summary.show(truncate=False)

    return {
        "raw_rows": raw_count,
        "transformed_rows": transformed_count,
    }


def write_outputs(enriched_df: DataFrame, segments_df: DataFrame, output_dir: Path) -> dict:
    """Write final data artifacts; writes are actions that trigger Spark jobs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    enriched_path = output_dir / "enriched_customers"
    segments_path = output_dir / "top_segments"

    logger.info("Writing enriched data to %s", enriched_path)
    enriched_df.write.mode("overwrite").parquet(str(enriched_path))

    logger.info("Writing top segments to %s", segments_path)
    segments_df.write.mode("overwrite").json(str(segments_path))

    return {
        "enriched_output": str(enriched_path),
        "segments_output": str(segments_path),
    }


def run_observable_pipeline(
    input_csv: Path = RAW_DATA_PATH,
    output_dir: Path = DATA_DIR / "processed" / "spark_ui_observable",
    persist_intermediate: bool = True,
    keep_ui_alive_seconds: int = 30,
) -> dict:
    """Execute full observable pipeline with optional caching and UI keep-alive."""
    spark = build_spark()
    ui_base_url = _normalize_local_ui_url(spark, spark.sparkContext.uiWebUrl)
    ui_jobs_url = f"{ui_base_url}/jobs" if ui_base_url else "http://localhost:4040/jobs"

    result = {
        "input_csv": str(input_csv),
        "output_dir": str(output_dir),
        "persist_intermediate": persist_intermediate,
        "keep_ui_alive_seconds": keep_ui_alive_seconds,
        "spark_ui_url": ui_jobs_url,
        "spark_app_id": spark.sparkContext.applicationId,
    }

    try:
        raw_df = load_data(spark, input_csv)
        transformed_df = transform_data(raw_df)

        if persist_intermediate:
            transformed_df = transformed_df.persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("Persisted transformed dataframe using MEMORY_AND_DISK")

        enriched_df = aggregate_metrics(transformed_df)
        segments_df = build_top_segments(enriched_df)

        result.update(materialize_checkpoints(raw_df, transformed_df, enriched_df))
        result.update(write_outputs(enriched_df, segments_df, output_dir))

        if keep_ui_alive_seconds > 0:
            logger.info("Keeping Spark session alive for %d seconds to inspect Spark UI", keep_ui_alive_seconds)
            time.sleep(keep_ui_alive_seconds)

        if persist_intermediate:
            transformed_df.unpersist()
            logger.info("Unpersisted transformed dataframe")

        result["status"] = "completed"
        return result
    finally:
        spark.stop()

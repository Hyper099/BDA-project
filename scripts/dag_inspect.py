from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("DAGInspection")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    input_path = "data/raw/financial_inclusion_data.csv"
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

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

    df = (
        df.withColumn(
            "digital_activity_score",
            col("monthly_upi_transactions") * col("avg_transaction_amount") / 1000.0,
        )
        .withColumn(
            "financial_discipline_score",
            (col("bill_payment_timeliness") + col("savings_ratio")) / 2.0,
        )
        .withColumn("is_underbanked", when(col("monthly_upi_transactions") < 8, 1).otherwise(0))
    )

    print("=== Spark Runtime ===")
    print("Spark version:", spark.version)

    print("\n=== Filesystem Config ===")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    print("fs.defaultFS:", hconf.get("fs.defaultFS"))
    print("fs.file.impl:", hconf.get("fs.file.impl"))
    print("fs.hdfs.impl:", hconf.get("fs.hdfs.impl"))

    print("\n=== Logical/Physical Plan (DAG-style plan) ===")
    df.explain(True)

    print("\n=== RDD Lineage (toDebugString DAG) ===")
    print(df.rdd.toDebugString().decode("utf-8"))

    print("\n=== Materialization Output ===")
    print("Row count:", df.count())
    df.select(
        "monthly_upi_transactions",
        "income",
        "digital_activity_score",
        "is_underbanked",
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

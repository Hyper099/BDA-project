from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FSCheckHDFS")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    print("fs.defaultFS:", hconf.get("fs.defaultFS"))
    print("fs.hdfs.impl:", hconf.get("fs.hdfs.impl"))
    spark.stop()


if __name__ == "__main__":
    main()

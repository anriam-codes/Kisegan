from pyspark.sql import functions as F
from pyspark.sql.types import *

from spark_session import get_spark_session


def get_cleaned_schema():
    return StructType([
        StructField("city", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("temperature_c", DoubleType()),
        StructField("feels_like_c", DoubleType()),
        StructField("temperature_min_c", DoubleType()),
        StructField("temperature_max_c", DoubleType()),
        StructField("humidity_pct", IntegerType()),
        StructField("pressure_hpa", IntegerType()),
        StructField("wind_speed_ms", DoubleType()),
        StructField("wind_direction_deg", IntegerType()),
        StructField("wind_gust_ms", DoubleType()),
        StructField("precip_mm", DoubleType()),
        StructField("rain_acc_mm", DoubleType()),
        StructField("cloud_cover_pct", IntegerType()),
        StructField("visibility_m", IntegerType()),
        StructField("event_time", TimestampType()),
        StructField("ingestion_time", TimestampType()),
        StructField("heat_index", DoubleType()),
        StructField("avg_temp", DoubleType()),
        StructField("event_id", StringType())
    ])


def start_storage_pipeline():
    spark = get_spark_session("Kisegan-Storage-Layer")

    # ----------------------------
    # MinIO S3A configuration
    # ----------------------------
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password123")
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # ----------------------------
    # Read from weather_cleaned
    # ----------------------------
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "weather_cleaned")
        .option("startingOffsets", "latest")
        .load()
    )

    # ----------------------------
    # Parse JSON
    # ----------------------------
    parsed_df = (
        kafka_df
        .select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json("json_str", get_cleaned_schema()).alias("data"))
        .select("data.*")
    )

    # ----------------------------
    # Partition columns
    # ----------------------------
    partitioned_df = (
        parsed_df
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

    # ----------------------------
    # Write Parquet to MinIO
    # ----------------------------
    query = (
        partitioned_df.writeStream
        .format("parquet")
        .option(
            "path",
            "s3a://weather-data/weather_events/"
        )
        .option(
            "checkpointLocation",
            "/tmp/checkpoints/weather_storage"
        )
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_storage_pipeline()
from pyspark.sql import functions as F

from .spark_session import get_spark_session
from .transform import transform_weather_stream


def start_weather_stream():
    spark = get_spark_session()

    # 1. Read from Kafka (weather_raw)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "weather_raw")
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Apply transformations
    normalized_df = transform_weather_stream(kafka_df)

    # 3. Write to Kafka (weather_cleaned)
    kafka_query = (
        normalized_df
        .selectExpr(
            "to_json(struct(*)) AS value"
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "weather_cleaned")
        .option("checkpointLocation", "/tmp/kisegan/checkpoints/weather_cleaned")
        .outputMode("append")
        .start()
    )

    kafka_query.awaitTermination()


if __name__ == "__main__":
    start_weather_stream()
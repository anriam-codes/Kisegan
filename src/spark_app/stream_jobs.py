from pyspark.sql import functions as F
from spark_app.spark_session import get_spark_session
from spark_app.transform import transform_weather_stream


def start_weather_stream():
    spark = get_spark_session()

    # Read from Kafka (weather_raw)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "weather_raw")
        .option("startingOffsets", "latest")
        .load()
    )

    # Transform
    normalized_df = transform_weather_stream(kafka_df)

    # Write back to Kafka (weather_cleaned)
    query = (
        normalized_df
        .selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "weather_cleaned")
        .option("checkpointLocation", "/tmp/kisegan/weather_cleaned")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_weather_stream()

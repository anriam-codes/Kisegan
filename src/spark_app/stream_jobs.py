from spark_app.spark_session import get_spark_session
from spark_app.transform import transform_weather_stream
from spark_app.sinks import write_to_mongo
import yaml


def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def start_weather_stream():
    spark = get_spark_session()

    # Load Mongo config
    spark_conf = load_yaml("config/spark.yaml")
    mongo_conf = spark_conf["mongo"]

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "weather_raw")
        .option("startingOffsets", "latest")
        .load()
    )

    # Transform
    cleaned_df = transform_weather_stream(kafka_df)

    # Kafka sink (cleaned topic)
    kafka_query = (
        cleaned_df
        .selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "weather_cleaned")
        .option("checkpointLocation", "/tmp/kisegan/weather_cleaned")
        .outputMode("append")
        .start()
    )

    # MongoDB sink (THIS was missing earlier)
    mongo_query = (
        cleaned_df
        .writeStream
        .foreachBatch(
            lambda df, batch_id: write_to_mongo(df, batch_id, mongo_conf)
        )
        .option("checkpointLocation", "/tmp/kisegan/weather_cleaned_mongo")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    start_weather_stream()

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from .schemas import raw_weather_schema


def transform_weather_stream(kafka_df):
    """
    Input  : Kafka streaming DataFrame
    Output : Normalized weather DataFrame
    """

    # 1. Parse Kafka JSON value
    parsed_df = (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                raw_weather_schema
            ).alias("json_data")
        )
        .select("json_data.*")
    )

    # 2. Flatten nested structure
    flat_df = parsed_df.select(
        F.col("location.name").alias("city"),
        F.col("location.lat").alias("latitude"),
        F.col("location.lon").alias("longitude"),

        F.col("data.values.temperature").alias("temperature_c"),
        F.col("data.values.humidity").alias("humidity_pct"),
        F.col("data.values.pressureSeaLevel").alias("pressure_hpa"),
        F.col("data.values.windSpeed").alias("wind_speed_ms"),
        F.col("data.values.windDirection").alias("wind_direction_deg"),

        F.col("data.time").alias("event_time_raw")
    )

    # 3. Timestamp normalization
    time_df = (
        flat_df
        .withColumn(
            "event_time",
            F.to_timestamp("event_time_raw")
        )
        .withColumn(
            "ingestion_time",
            F.current_timestamp()
        )
        .drop("event_time_raw")
    )

    # 4. Basic null handling (critical fields)
    cleaned_df = time_df.dropna(
        subset=["city", "temperature_c", "event_time"]
    )

    return cleaned_df
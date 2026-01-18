from pyspark.sql import functions as F
from spark_app.schemas import raw_weather_schema

# NOTE:
# Units are assumed to be standardized from source:
# temperature: Celsius
# wind_speed: m/s
# pressure: hPa
# Explicit unit conversion will be handled in analytics layer

def transform_weather_stream(kafka_df):
    # 1. Kafka value (binary) → string
    json_df = kafka_df.select(
        F.col("value").cast("string").alias("json_str")
    )

    # 2. Parse JSON
    parsed_df = (
        json_df
        .select(F.from_json("json_str", raw_weather_schema).alias("json"))
        .select("json.*")
    )

    # 3. Flatten based on ACTUAL message structure
    flat_df = parsed_df.select(
        F.col("location.name").alias("city"),
        F.col("location.lat").alias("latitude"),
        F.col("location.lon").alias("longitude"),

        F.col("data.temperature").alias("temperature_c"),
        F.col("data.humidity").alias("humidity_pct"),
        F.col("data.pressureSurfaceLevel").alias("pressure_hpa"),
        F.col("data.windSpeed").alias("wind_speed_ms"),
        F.col("data.windDirection").alias("wind_direction_deg"),

        # observationTime is UNIX seconds
        F.to_timestamp(F.col("data.observationTime")).alias("event_time"),
        F.current_timestamp().alias("ingestion_time")
    )

    # 4. Drop invalid rows
    cleaned_df = flat_df.dropna(
        subset=["city", "temperature_c", "event_time"]
    )

    return cleaned_df

from pyspark.sql import functions as F
from spark_app.schemas import raw_weather_schema


def transform_weather_stream(kafka_df):
    # 1. Kafka value → string
    json_df = kafka_df.select(
        F.col("value").cast("string").alias("json_str")
    )

    # 2. Parse JSON
    parsed_df = (
        json_df
        .select(F.from_json("json_str", raw_weather_schema).alias("json"))
        .select("json.*")
    )

    # 3. Flatten + normalize
    flat_df = parsed_df.select(
        F.col("location.name").alias("city"),
        F.col("location.lat").alias("latitude"),
        F.col("location.lon").alias("longitude"),

        F.col("data.temperature").alias("temperature_c"),
        F.col("data.temperatureApparent").alias("feels_like_c"),
        F.col("data.temperatureMin").alias("temperature_min_c"),
        F.col("data.temperatureMax").alias("temperature_max_c"),

        F.col("data.humidity").alias("humidity_pct"),
        F.col("data.pressureSurfaceLevel").alias("pressure_hpa"),

        F.col("data.windSpeed").alias("wind_speed_ms"),
        F.col("data.windDirection").alias("wind_direction_deg"),
        F.col("data.windGust").alias("wind_gust_ms"),

        F.coalesce(
            F.col("data.precipitationIntensity"), F.lit(0.0)
        ).alias("precip_mm"),

        F.coalesce(
            F.col("data.rainAccumulation"), F.lit(0.0)
        ).alias("rain_acc_mm"),

        F.col("data.cloudCover").alias("cloud_cover_pct"),
        F.col("data.visibility").alias("visibility_m"),

        # UNIX seconds → timestamp
        F.to_timestamp(F.from_unixtime(F.col("data.observationTime"))).alias("event_time"),
        F.current_timestamp().alias("ingestion_time")
    )

    # 4. Minimal row validity check
    cleaned_df = flat_df.dropna(
        subset=["city", "temperature_c", "event_time"]
    )

    return cleaned_df
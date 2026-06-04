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
        F.to_timestamp(
            F.from_unixtime(F.col("data.observationTime"))
        ).alias("event_time"),

        F.from_utc_timestamp(
            F.current_timestamp(),
            "Asia/Kolkata"
        ).alias("ingestion_time")
    )

    # 4. Minimal row validity check
    cleaned_df = flat_df.dropna(
        subset=["city", "temperature_c", "event_time"]
    )

    # 5. Data quality validation checks
    validated_df = cleaned_df.filter(
        (F.col("temperature_c").between(-50, 60)) &
        (F.col("humidity_pct").between(0, 100)) &
        (F.col("pressure_hpa").between(800, 1200)) &
        (F.col("wind_speed_ms").between(0, 150))
    )

    # 6. Lightweight feature engineering
    featured_df = (
        validated_df
        .withColumn(
            "heat_index",
            F.col("temperature_c")
            + 0.33 * F.col("humidity_pct")
            - 0.7 * F.col("wind_speed_ms")
            - 4
        )
        .withColumn(
            "avg_temp",
            F.col("temperature_c")
        )
        .withColumn(
            "event_id",
            F.concat_ws(
                "_",
                F.col("city"),
                F.date_format(
                    F.from_utc_timestamp(F.col("event_time"), "Asia/Kolkata"),
                    "yyyy-MM-dd'T'HH:mm:ssXXX"
                )
            )
        )
    )

    return featured_df
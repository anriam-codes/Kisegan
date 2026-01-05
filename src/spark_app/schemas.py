from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

raw_weather_schema = StructType([
    StructField("data", StructType([
        StructField("time", StringType(), True),
        StructField("values", StructType([
            StructField("temperature", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("pressureSeaLevel", DoubleType(), True),
            StructField("windSpeed", DoubleType(), True),
            StructField("windDirection", IntegerType(), True)
        ]), True)
    ]), True),

    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("name", StringType(), True)
    ]), True)
])

normalized_weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", IntegerType(), True),
    StructField("pressure_hpa", DoubleType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
    StructField("wind_direction_deg", IntegerType(), True),

    StructField("event_time", TimestampType(), True),
    StructField("ingestion_time", TimestampType(), True)
])

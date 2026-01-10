from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType
)

raw_weather_schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),

    StructField("data", StructType([
        StructField("observationTime", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("temperatureMin", DoubleType(), True),
        StructField("temperatureMax", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("pressureSurfaceLevel", IntegerType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("windDirection", IntegerType(), True),
        StructField("cloudCover", IntegerType(), True),
        StructField("visibility", IntegerType(), True),
    ]), True)
])
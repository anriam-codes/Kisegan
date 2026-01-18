from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "Kisegan-Weather-Stream"):
    spark = (
        SparkSession.builder
        .appName(app_name)

        # ADD THIS (this was missing)
        # MongoDB connection
        .config(
            "spark.mongodb.read.connection.uri",
            "mongodb://mongodb:27017/weather"
        )
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017/weather"
        )

        # Stability configs
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.local.dir", "/tmp/spark-temp")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

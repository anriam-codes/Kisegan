from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "Kisegan-Weather-Stream"):
    spark = (
        SparkSession.builder
        .appName(app_name)

        # Kafka (version must match Spark 3.4.x)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )

        # MongoDB
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://localhost:27017/kisegan"
        )

        # Time handling
        .config("spark.sql.session.timeZone", "UTC")

        # Windows fixes
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.local.dir", "C:/spark-temp")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
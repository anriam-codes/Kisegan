from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "Kisegan-Weather-Stream"):
    spark = (
        SparkSession.builder
        .appName(app_name)

        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

        .config("spark.mongodb.write.connection.uri",
                "mongodb://localhost:27017/kisegan")

        .config("spark.sql.streaming.checkpointLocation",
                "/tmp/kisegan/checkpoints")

        .config("spark.sql.session.timeZone", "UTC")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
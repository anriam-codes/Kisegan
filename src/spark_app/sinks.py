from pyspark.sql import DataFrame

def write_to_mongo(batch_df: DataFrame, batch_id: int, mongo_conf: dict):
    batch_df.persist()

    try:
        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("spark.mongodb.write.connection.uri", mongo_conf["uri"])
            .option("spark.mongodb.write.database", mongo_conf["database"])
            .option("spark.mongodb.write.collection", mongo_conf["collection"])
            .save()
        )
    finally:
        batch_df.unpersist()
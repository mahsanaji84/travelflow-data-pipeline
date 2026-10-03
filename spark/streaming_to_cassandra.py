from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

spark = (
    SparkSession.builder
    .appName("TravelFlowStreamingToCassandra")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("package_id", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Lecture du flux Kafka
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "travelflow-events")
    .option("startingOffsets", "latest")
    .load()
)

# Conversion du message Kafka en texte JSON
df_json = df_kafka.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

# Transformation JSON -> colonnes structurées
df_events = (
    df_json
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

# Agrégation par destination
stats_destination = (
    df_events
    .groupBy("destination")
    .count()
    .withColumnRenamed("count", "event_count")
)

# Fonction exécutée pour chaque micro-batch
def write_to_cassandra(batch_df, batch_id):

    print(f"\n========== BATCH {batch_id} ==========")

    nb_rows = batch_df.count()

    print(f"Nombre de destinations dans le batch : {nb_rows}")

    if nb_rows > 0:

        batch_df.show(truncate=False)

        (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(
                table="destination_stats",
                keyspace="travelflow"
            )
            .save()
        )

        print("Écriture dans Cassandra terminée.")

    else:
        print("Batch vide.")

query = (
    stats_destination.writeStream
    .foreachBatch(write_to_cassandra)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "/tmp/travelflow-checkpoint"
    )
    .start()
)

query.awaitTermination()
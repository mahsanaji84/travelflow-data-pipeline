from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("TravelFlowStreamingAnalytics")
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

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "travelflow-events")
    .option("startingOffsets", "latest")
    .load()
)

df_json = df_kafka.selectExpr(
    "CAST(value AS STRING) as json_value"
)

df_events = (
    df_json
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

stats_destination = (
    df_events
    .groupBy("destination")
    .count()
)

query_destination = (
    stats_destination.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)

query_destination.awaitTermination()
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TravelFlowCassandraTest")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

data = [
    ("Montreal", 10),
    ("Paris", 7),
    ("Rome", 5)
]

df = spark.createDataFrame(
    data,
    ["destination", "event_count"]
)

print("DataFrame à écrire dans Cassandra :")
df.show()

(
    df.write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .options(
        table="destination_stats",
        keyspace="travelflow"
    )
    .save()
)

print("Écriture Cassandra terminée avec succès.")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import findspark
findspark.init()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoTrendAnalyzer") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for incoming Kafka JSON messages
schema = StructType([
    StructField("symbol",    StringType(), True),
    StructField("price",     DoubleType(), True),
    StructField("volume",    DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# Read from Kafka topic: crypto-stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON values
df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")

# Convert timestamp
df = df.withColumn("event_time", to_timestamp("timestamp"))

# Moving average (last 5 events per symbol)
window_spec = Window.partitionBy("symbol") \
    .orderBy("event_time") \
    .rowsBetween(-4, 0)

df = df.withColumn("moving_avg", avg("price").over(window_spec))

# Percentage change
df = df.withColumn("pct_change",
    ((col("price") - col("moving_avg")) / col("moving_avg") * 100))

# Final schema matching Hive table
df_out = df.select(
    col("symbol"),
    col("price"),
    col("moving_avg"),
    col("volume"),
    col("pct_change"),
    col("event_time").alias("timestamp")
)

# Write to Kafka topic: crypto-analyzed
df_out.selectExpr(
    "symbol AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "crypto-analyzed") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-kafka") \
    .outputMode("append") \
    .start()

# Write to HDFS as Parquet (for Hive)
df_out.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/hive/warehouse/crypto_db.db/crypto_analysis") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-hdfs") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
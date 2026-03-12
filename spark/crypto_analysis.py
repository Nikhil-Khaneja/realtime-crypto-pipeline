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

# Updated schema matching CoinGecko producer output
schema = StructType([
    StructField("coin_id",          StringType(), True),
    StructField("symbol",           StringType(), True),
    StructField("name",             StringType(), True),
    StructField("price",            DoubleType(), True),
    StructField("market_cap",       DoubleType(), True),
    StructField("volume_24h",       DoubleType(), True),
    StructField("high_24h",         DoubleType(), True),
    StructField("low_24h",          DoubleType(), True),
    StructField("price_change_1h",  DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("timestamp",        StringType(), True),
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
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

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

# Final schema for Hive table
df_out = df.select(
    col("symbol"),
    col("name"),
    col("price"),
    col("moving_avg"),
    col("volume_24h").alias("volume"),
    col("market_cap"),
    col("high_24h"),
    col("low_24h"),
    col("price_change_1h"),
    col("price_change_24h"),
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
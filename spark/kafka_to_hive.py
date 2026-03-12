"""
3.2.c — Store and Access Data
Reads analyzed crypto data from Kafka (crypto-analyzed topic) using
Spark Streaming and stores it in a Hive table in Parquet format on HDFS.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    KAFKA_BROKER, KAFKA_ANALYZED_TOPIC,
    HIVE_DATABASE, HIVE_TABLE, HDFS_WAREHOUSE,
)


# ---------------------------------------------------------------------------
# Schema for analyzed messages from crypto_analysis.py
# ---------------------------------------------------------------------------

ANALYZED_SCHEMA = StructType([
    StructField("symbol",          StringType(), True),
    StructField("name",            StringType(), True),
    StructField("window_start",    StringType(), True),
    StructField("window_end",      StringType(), True),
    StructField("avg_price_5m",    DoubleType(), True),
    StructField("high_24h",        DoubleType(), True),
    StructField("low_24h",         DoubleType(), True),
    StructField("avg_volume_5m",   DoubleType(), True),
    StructField("avg_change_1h",   DoubleType(), True),
    StructField("avg_change_24h",  DoubleType(), True),
    StructField("volatility_pct",  DoubleType(), True),
    StructField("avg_market_cap",  DoubleType(), True),
    StructField("trend_signal",    StringType(), True),
    StructField("analyzed_at",     StringType(), True),
])


# ---------------------------------------------------------------------------
# Spark session with Hive support
# ---------------------------------------------------------------------------

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoKafkaToHive")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", HDFS_WAREHOUSE)
        .enableHiveSupport()
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Ensure Hive table exists
# ---------------------------------------------------------------------------

def ensure_hive_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    spark.sql(f"USE {HIVE_DATABASE}")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_TABLE} (
            symbol          STRING,
            name            STRING,
            window_start    STRING,
            window_end      STRING,
            avg_price_5m    DOUBLE,
            high_24h        DOUBLE,
            low_24h         DOUBLE,
            avg_volume_5m   DOUBLE,
            avg_change_1h   DOUBLE,
            avg_change_24h  DOUBLE,
            volatility_pct  DOUBLE,
            avg_market_cap  DOUBLE,
            trend_signal    STRING,
            analyzed_at     STRING
        )
        STORED AS PARQUET
        LOCATION '{HDFS_WAREHOUSE}'
    """)
    print(f"[Hive] Table {HIVE_DATABASE}.{HIVE_TABLE} ready.")


# ---------------------------------------------------------------------------
# Write each micro-batch to Hive
# ---------------------------------------------------------------------------

def write_to_hive(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    (
        batch_df
        .write
        .mode("append")
        .format("parquet")
        .save(HDFS_WAREHOUSE)
    )
    print(f"[Hive] Batch {batch_id}: wrote {count} rows → {HDFS_WAREHOUSE}")


# ---------------------------------------------------------------------------
# Streaming pipeline
# ---------------------------------------------------------------------------

def run_pipeline(spark):
    ensure_hive_table(spark)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_ANALYZED_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), ANALYZED_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("ingested_at", current_timestamp())
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_to_hive)
        .option("checkpointLocation", "/tmp/checkpoints/kafka-to-hive")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[Pipeline] '{KAFKA_ANALYZED_TOPIC}' → Hive '{HIVE_DATABASE}.{HIVE_TABLE}'")

    query = run_pipeline(spark)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Pipeline] Stopping.")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

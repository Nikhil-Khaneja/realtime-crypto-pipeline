"""
3.2.b — Real-time Streaming Data Analysis with Spark
Reads raw crypto data from Kafka (crypto-raw topic), computes market trend
indicators (moving averages, VWAP, momentum, trend signal), and writes
analyzed results back to Kafka (crypto-analyzed topic).
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    avg, max as spark_max, min as spark_min,
    window, expr, round as spark_round,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_BROKER, KAFKA_RAW_TOPIC, KAFKA_ANALYZED_TOPIC


# ---------------------------------------------------------------------------
# Schema for raw Kafka messages from coingecko_producer
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType([
    StructField("coin_id",            StringType(),  True),
    StructField("symbol",             StringType(),  True),
    StructField("name",               StringType(),  True),
    StructField("price",              DoubleType(),  True),
    StructField("market_cap",         DoubleType(),  True),
    StructField("volume_24h",         DoubleType(),  True),
    StructField("high_24h",           DoubleType(),  True),
    StructField("low_24h",            DoubleType(),  True),
    StructField("price_change_1h",    DoubleType(),  True),
    StructField("price_change_24h",   DoubleType(),  True),
    StructField("circulating_supply", DoubleType(),  True),
    StructField("timestamp",          StringType(),  True),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoTrendAnalysis")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .config("spark.sql.shuffle.partitions", "2")  # low for single node
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Stream processing
# ---------------------------------------------------------------------------

def run_analysis(spark: SparkSession):
    # Read raw stream from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON payload
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
    )

    # 5-minute rolling window — aggregated trend indicators per coin
    windowed = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            col("symbol"),
            col("name"),
            window(col("event_time"), "5 minutes", "1 minute"),
        )
        .agg(
            spark_round(avg("price"),          6).alias("avg_price_5m"),
            spark_round(spark_max("high_24h"), 6).alias("high_24h"),
            spark_round(spark_min("low_24h"),  6).alias("low_24h"),
            spark_round(avg("volume_24h"),     2).alias("avg_volume_5m"),
            spark_round(avg("price_change_1h"),  4).alias("avg_change_1h"),
            spark_round(avg("price_change_24h"), 4).alias("avg_change_24h"),
            spark_round(avg("market_cap"),     2).alias("avg_market_cap"),
        )
    )

    # Derived indicators
    enriched = (
        windowed
        # Volatility: (high - low) / low * 100
        .withColumn(
            "volatility_pct",
            spark_round(
                (col("high_24h") - col("low_24h")) / col("low_24h") * 100, 4
            ),
        )
        # Trend signal based on 1h and 24h price change
        .withColumn(
            "trend_signal",
            expr("""
                CASE
                    WHEN avg_change_1h > 1.0 AND avg_change_24h > 2.0  THEN 'STRONG_BULLISH'
                    WHEN avg_change_1h > 0   AND avg_change_24h > 0    THEN 'BULLISH'
                    WHEN avg_change_1h < -1.0 AND avg_change_24h < -2.0 THEN 'STRONG_BEARISH'
                    WHEN avg_change_1h < 0   AND avg_change_24h < 0    THEN 'BEARISH'
                    ELSE 'NEUTRAL'
                END
            """),
        )
        .withColumn("analyzed_at", current_timestamp())
        .select(
            col("symbol"),
            col("name"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_price_5m"),
            col("high_24h"),
            col("low_24h"),
            col("avg_volume_5m"),
            col("avg_change_1h"),
            col("avg_change_24h"),
            col("volatility_pct"),
            col("avg_market_cap"),
            col("trend_signal"),
            col("analyzed_at"),
        )
    )

    # Write analyzed results to Kafka
    kafka_query = (
        enriched
        .select(
            col("symbol").alias("key"),
            to_json(struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", KAFKA_ANALYZED_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/crypto-analysis")
        .outputMode("update")
        .start()
    )

    # Also print to console for monitoring
    console_query = (
        enriched
        .writeStream
        .format("console")
        .option("truncate", False)
        .outputMode("update")
        .start()
    )

    return kafka_query, console_query


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Analysis] '{KAFKA_RAW_TOPIC}' → trend analysis → '{KAFKA_ANALYZED_TOPIC}'")

    kafka_q, console_q = run_analysis(spark)
    try:
        kafka_q.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Analysis] Stopping.")
    finally:
        kafka_q.stop()
        console_q.stop()
        spark.stop()


if __name__ == "__main__":
    main()

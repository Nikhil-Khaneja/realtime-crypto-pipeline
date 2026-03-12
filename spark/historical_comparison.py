"""
3.3 — Comparative Market Trend Analyzer
Fetches historical crypto data from CoinGecko REST API and compares
it with real-time data from Kafka to identify trends over time.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, round as spark_round,
    current_timestamp, lit, when,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    COINGECKO_API_KEY, COINGECKO_BASE_URL,
    KAFKA_BROKER, KAFKA_RAW_TOPIC, COIN_IDS,
)


# ---------------------------------------------------------------------------
# Fetch historical OHLC data from CoinGecko (last 7 days)
# ---------------------------------------------------------------------------

def fetch_historical(coin_id: str, days: int = 7) -> list:
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/ohlc"
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}
    params = {"vs_currency": "usd", "days": days}

    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()

    rows = []
    for entry in resp.json():
        ts, open_, high, low, close = entry
        rows.append({
            "coin_id":    coin_id,
            "timestamp":  datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
            "open":       open_,
            "high":       high,
            "low":        low,
            "close":      close,
            "type":       "historical",
        })
    return rows


def fetch_all_historical() -> list:
    all_rows = []
    for coin_id in COIN_IDS:
        print(f"[Historical] Fetching {coin_id} (7-day OHLC)...")
        rows = fetch_historical(coin_id)
        all_rows.extend(rows)
        time.sleep(1)  # respect rate limit
    return all_rows


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoHistoricalComparison")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Historical analysis
# ---------------------------------------------------------------------------

HISTORICAL_SCHEMA = StructType([
    StructField("coin_id",   StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open",      DoubleType(), True),
    StructField("high",      DoubleType(), True),
    StructField("low",       DoubleType(), True),
    StructField("close",     DoubleType(), True),
    StructField("type",      StringType(), True),
])

RAW_SCHEMA = StructType([
    StructField("coin_id",          StringType(), True),
    StructField("symbol",           StringType(), True),
    StructField("price",            DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("volume_24h",       DoubleType(), True),
    StructField("timestamp",        StringType(), True),
])


def analyze_historical(spark, historical_data: list):
    hist_df = spark.createDataFrame(historical_data, schema=HISTORICAL_SCHEMA)

    # 7-day summary per coin
    summary = (
        hist_df.groupBy("coin_id")
        .agg(
            spark_round(avg("close"),  2).alias("avg_price_7d"),
            spark_round(avg("high"),   2).alias("avg_high_7d"),
            spark_round(avg("low"),    2).alias("avg_low_7d"),
        )
    )

    print("\n[Historical] 7-Day Price Summary:")
    summary.show(truncate=False)
    return summary


def compare_with_realtime(spark, hist_summary):
    # Fetch one batch of real-time data from Kafka
    realtime_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), RAW_SCHEMA).alias("d"))
        .select("d.*")
    )

    # Latest price per coin
    latest = (
        realtime_df.groupBy("coin_id")
        .agg(spark_round(avg("price"), 2).alias("current_price"))
    )

    # Join historical summary with real-time
    comparison = (
        hist_summary.join(latest, on="coin_id", how="inner")
        .withColumn(
            "vs_7d_avg_pct",
            spark_round(
                (col("current_price") - col("avg_price_7d")) / col("avg_price_7d") * 100, 2
            ),
        )
        .withColumn(
            "trend_vs_history",
            when(col("vs_7d_avg_pct") > 5,  "ABOVE_TREND")
            .when(col("vs_7d_avg_pct") < -5, "BELOW_TREND")
            .otherwise("ON_TREND"),
        )
        .withColumn("compared_at", current_timestamp())
    )

    print("\n[Comparison] Real-time vs 7-Day Historical:")
    comparison.select(
        "coin_id", "avg_price_7d", "current_price",
        "vs_7d_avg_pct", "trend_vs_history"
    ).show(truncate=False)

    return comparison


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[Historical] Fetching 7-day historical data from CoinGecko...")
    historical_data = fetch_all_historical()
    print(f"[Historical] Fetched {len(historical_data)} OHLC records.")

    hist_summary = analyze_historical(spark, historical_data)
    compare_with_realtime(spark, hist_summary)

    spark.stop()


if __name__ == "__main__":
    main()

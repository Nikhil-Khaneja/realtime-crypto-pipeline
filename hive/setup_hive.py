"""
Hive table setup using PyHive (Python instead of beeline/HQL).
Creates the crypto database and crypto_data table in Parquet format on HDFS.
Run this once before starting the pipeline.
"""

import sys
import os
from pyhive import hive

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import HIVE_HOST, HIVE_PORT, HIVE_DATABASE, HIVE_TABLE, HDFS_WAREHOUSE


def get_connection():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, auth="NONE")


def setup():
    conn = get_connection()
    cursor = conn.cursor()

    print(f"[Hive] Connected to {HIVE_HOST}:{HIVE_PORT}")

    # Create database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    print(f"[Hive] Database '{HIVE_DATABASE}' ready.")

    cursor.execute(f"USE {HIVE_DATABASE}")

    # Create external Parquet table
    cursor.execute(f"""
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
    print(f"[Hive] Table '{HIVE_TABLE}' ready at {HDFS_WAREHOUSE}")

    # Verify
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"[Hive] Tables in '{HIVE_DATABASE}': {[t[0] for t in tables]}")

    cursor.close()
    conn.close()
    print("[Hive] Setup complete.")


if __name__ == "__main__":
    setup()

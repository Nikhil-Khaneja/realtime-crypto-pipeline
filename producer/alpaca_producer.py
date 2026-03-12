"""
3.2.a — Real-time Stream Consumer
Consumes live crypto bar data from Alpaca WebSocket API,
preprocesses each message, and publishes to Kafka.
"""

import json
import re
import sys
import os
import asyncio
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError
from alpaca.data.live import CryptoDataStream

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY,
    KAFKA_BROKER, KAFKA_RAW_TOPIC, SYMBOLS,
)


# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------

def create_kafka_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaConnectionError as e:
            print(f"[Producer] Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            import time; time.sleep(3)


# ---------------------------------------------------------------------------
# Preprocessing helpers
# ---------------------------------------------------------------------------

def preprocess(raw: dict) -> dict | None:
    """
    Clean and enrich a raw bar message.
    Returns None to drop the message if it fails validation.
    """
    # Drop records with missing price or volume
    if raw.get("price") is None or raw.get("volume") is None:
        return None
    if raw["price"] <= 0 or raw["volume"] < 0:
        return None

    symbol = raw.get("symbol", "").strip().upper()
    # Normalise symbol format: "BTCUSD" → "BTC/USD"
    symbol = re.sub(r"^([A-Z]{3})([A-Z]{3})$", r"\1/\2", symbol)

    return {
        "symbol":     symbol,
        "price":      round(float(raw["price"]), 6),
        "open":       round(float(raw.get("open", raw["price"])), 6),
        "high":       round(float(raw.get("high", raw["price"])), 6),
        "low":        round(float(raw.get("low",  raw["price"])), 6),
        "volume":     round(float(raw["volume"]), 4),
        "trade_count": int(raw.get("trade_count", 0)),
        "vwap":       round(float(raw.get("vwap", raw["price"])), 6),
        "timestamp":  raw.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Alpaca WebSocket handler
# ---------------------------------------------------------------------------

kafka_producer = create_kafka_producer()


async def handle_bar(bar) -> None:
    raw = {
        "symbol":      bar.symbol,
        "price":       bar.close,
        "open":        bar.open,
        "high":        bar.high,
        "low":         bar.low,
        "volume":      bar.volume,
        "trade_count": getattr(bar, "trade_count", 0),
        "vwap":        getattr(bar, "vwap", bar.close),
        "timestamp":   bar.timestamp.isoformat() if hasattr(bar.timestamp, "isoformat") else str(bar.timestamp),
    }

    message = preprocess(raw)
    if message is None:
        print(f"[Producer] Dropped invalid bar for {raw.get('symbol')}")
        return

    future = kafka_producer.send(KAFKA_RAW_TOPIC, value=message)
    try:
        future.get(timeout=5)
        print(f"[Producer] {message['symbol']} | price={message['price']} | vol={message['volume']} → {KAFKA_RAW_TOPIC}")
    except Exception as e:
        print(f"[Producer] Failed to send message: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise EnvironmentError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env"
        )

    stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    # Subscribe to minute bars for each symbol
    for symbol in SYMBOLS:
        stream.subscribe_bars(handle_bar, symbol)

    print(f"[Producer] Streaming {SYMBOLS} → Kafka topic '{KAFKA_RAW_TOPIC}' ...")
    try:
        stream.run()
    except KeyboardInterrupt:
        print("\n[Producer] Shutting down.")
    finally:
        kafka_producer.flush()
        kafka_producer.close()


if __name__ == "__main__":
    main()

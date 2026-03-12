"""
3.2.a — Real-time Stream Consumer
Polls CoinGecko API for live crypto market data,
preprocesses each message, and publishes to Kafka.
"""

import json
import time
import sys
import os
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    COINGECKO_API_KEY, COINGECKO_BASE_URL,
    KAFKA_BROKER, KAFKA_RAW_TOPIC,
    COIN_IDS, POLL_INTERVAL,
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
            time.sleep(3)


# ---------------------------------------------------------------------------
# CoinGecko API fetch
# ---------------------------------------------------------------------------

def fetch_market_data() -> list:
    """Fetch real-time market data for all tracked coins."""
    url = f"{COINGECKO_BASE_URL}/coins/markets"
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COIN_IDS),
        "order": "market_cap_desc",
        "sparkline": False,
        "price_change_percentage": "1h,24h",
    }

    response = requests.get(url, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def preprocess(coin: dict):
    """Clean and normalize a raw CoinGecko coin record."""
    price = coin.get("current_price")
    volume = coin.get("total_volume")

    if price is None or price <= 0:
        return None
    if volume is None or volume < 0:
        return None

    return {
        "coin_id":            coin.get("id", "").lower(),
        "symbol":             coin.get("symbol", "").upper(),
        "name":               coin.get("name", ""),
        "price":              round(float(price), 6),
        "market_cap":         float(coin.get("market_cap") or 0),
        "volume_24h":         round(float(volume), 2),
        "high_24h":           round(float(coin.get("high_24h") or price), 6),
        "low_24h":            round(float(coin.get("low_24h") or price), 6),
        "price_change_1h":    round(float(coin.get("price_change_percentage_1h_in_currency") or 0), 4),
        "price_change_24h":   round(float(coin.get("price_change_percentage_24h") or 0), 4),
        "circulating_supply": float(coin.get("circulating_supply") or 0),
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def main() -> None:
    if not COINGECKO_API_KEY:
        raise EnvironmentError("COINGECKO_API_KEY must be set in .env")

    producer = create_kafka_producer()
    print(f"[Producer] Polling CoinGecko every {POLL_INTERVAL}s → Kafka topic '{KAFKA_RAW_TOPIC}'")
    print(f"[Producer] Tracking: {COIN_IDS}")

    while True:
        try:
            coins = fetch_market_data()
            for coin in coins:
                message = preprocess(coin)
                if message is None:
                    print(f"[Producer] Dropped invalid data for {coin.get('id')}")
                    continue

                future = producer.send(KAFKA_RAW_TOPIC, value=message)
                future.get(timeout=5)
                print(
                    f"[Producer] {message['symbol']} | "
                    f"price=${message['price']:,.2f} | "
                    f"24h change={message['price_change_24h']}% | "
                    f"vol=${message['volume_24h']:,.0f}"
                )

            producer.flush()

        except requests.exceptions.RequestException as e:
            print(f"[Producer] API error: {e}")
        except Exception as e:
            print(f"[Producer] Unexpected error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Producer] Shutting down.")

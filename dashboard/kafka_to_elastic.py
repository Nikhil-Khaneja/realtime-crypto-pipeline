"""
3.2.d — Visualization
Reads real-time crypto data from Kafka (crypto-stream topic) and
indexes it into Elasticsearch for visualization in Kibana.
"""

import json
import sys
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_BROKER, KAFKA_RAW_TOPIC

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ES_HOST  = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = "crypto-prices"


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def create_es_client():
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Cannot connect to Elasticsearch at {ES_HOST}")
    print(f"[Elastic] Connected to {ES_HOST}")
    return es


def create_index(es: Elasticsearch):
    if es.indices.exists(index=ES_INDEX):
        return
    es.indices.create(
        index=ES_INDEX,
        body={
            "mappings": {
                "properties": {
                    "coin_id":          {"type": "keyword"},
                    "symbol":           {"type": "keyword"},
                    "name":             {"type": "keyword"},
                    "price":            {"type": "float"},
                    "market_cap":       {"type": "float"},
                    "volume_24h":       {"type": "float"},
                    "high_24h":         {"type": "float"},
                    "low_24h":          {"type": "float"},
                    "price_change_1h":  {"type": "float"},
                    "price_change_24h": {"type": "float"},
                    "timestamp":        {"type": "date"},
                }
            }
        },
    )
    print(f"[Elastic] Index '{ES_INDEX}' created.")


def create_consumer():
    return KafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="elastic-indexer",
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    es = create_es_client()
    create_index(es)

    consumer = create_consumer()
    print(f"[Elastic] Consuming from '{KAFKA_RAW_TOPIC}' → indexing to '{ES_INDEX}'")

    for msg in consumer:
        doc = msg.value
        doc["@timestamp"] = doc.get("timestamp", datetime.now(timezone.utc).isoformat())

        es.index(index=ES_INDEX, document=doc)
        print(
            f"[Elastic] Indexed {doc.get('symbol')} | "
            f"price=${doc.get('price'):,.2f} | "
            f"change_24h={doc.get('price_change_24h')}%"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Elastic] Stopped.")

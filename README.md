# Real-Time Crypto Pipeline

A real-time cryptocurrency data pipeline built on AWS EC2, combining Kafka, PySpark Streaming, Hive/HDFS, Elasticsearch, and Kibana to ingest, process, store, and visualize live market data.

## Architecture

```
CoinGecko API / Alpaca WebSocket
         │
         ▼
   Kafka Producer
   (crypto-stream topic)
         │
         ├──────────────────────────────────┐
         ▼                                  ▼
  PySpark Streaming                 Kafka → Elasticsearch
  (crypto_analysis.py)              (kafka_to_elastic.py)
  Computes: VWAP, moving                    │
  averages, momentum,                       ▼
  trend signals                          Kibana
         │                            (live dashboard)
         ▼
  Kafka (crypto-analyzed topic)
         │
         ├──────────────────────────────────┐
         ▼                                  ▼
  PySpark → Hive/HDFS             Historical Comparison
  (kafka_to_hive.py)              (historical_comparison.py)
  Parquet storage                 CoinGecko REST vs Kafka
```

## Stack

| Layer | Technology |
|---|---|
| Cloud | AWS EC2 (t2.micro, Amazon Linux 2023) |
| Message Broker | Apache Kafka 3.5.1 + Zookeeper |
| Stream Processing | PySpark 3.4+ Structured Streaming |
| Storage | Apache Hive 3.1.3 on Hadoop HDFS 3.3.6 (Parquet) |
| Indexing | Elasticsearch 8+ |
| Visualization | Kibana |
| Data Sources | CoinGecko REST API, Alpaca WebSocket |
| Runtime | Python 3.9, Java 8 (Corretto) |

## Components

### Producers (`producer/`)
- **`coingecko_producer.py`** — Polls CoinGecko `/coins/markets` every 30s for BTC, ETH, SOL; preprocesses and publishes to `crypto-stream` Kafka topic.
- **`alpaca_producer.py`** — Streams live crypto bar data from Alpaca WebSocket and publishes to the same Kafka topic.

### Stream Processing (`spark/`)
- **`crypto_analysis.py`** — Reads from `crypto-stream`, computes 5-min windowed VWAP, moving averages, price momentum, and a BULLISH/BEARISH trend signal; writes results to `crypto-analyzed` topic.
- **`kafka_to_hive.py`** — Reads `crypto-analyzed`, sinks to a Hive table (`crypto.crypto_data`) in Parquet format on HDFS for durable storage and batch queries.
- **`historical_comparison.py`** — Fetches 30-day historical data from CoinGecko REST API and joins it with real-time Kafka data to identify trend divergences.

### Dashboard (`dashboard/`)
- **`kafka_to_elastic.py`** — Consumes `crypto-stream` and indexes each message into Elasticsearch; Kibana visualizes the live feed.

### Config (`config/`)
- **`settings.py`** — Central config: API keys (from `.env`), Kafka broker address, topic names, Hive connection, HDFS warehouse path, coin list, poll interval.
- **`kafka-config.properties`** — Kafka broker/topic configuration.

### Hive (`hive/`)
- **`setup_hive.py`** — Creates the `crypto` database and `crypto_data` Hive table via PyHive.

## Data Flow

1. Producers fetch/stream crypto prices and push JSON messages to `crypto-stream`.
2. PySpark reads `crypto-stream` as a structured stream and computes trend indicators per coin per 5-minute window.
3. Enriched records land in `crypto-analyzed`.
4. A second Spark job drains `crypto-analyzed` into Hive (Parquet on HDFS) for historical queries.
5. In parallel, the Elasticsearch indexer writes raw records to an ES index; Kibana renders price/volume charts in real time.
6. The historical comparison job periodically fetches REST data and overlays it against live stream data to surface long-term vs. short-term trend signals.

## Setup

See [docs/setup-guide.md](docs/setup-guide.md) for the full step-by-step guide covering:
- EC2 provisioning and EBS volume expansion
- Hadoop/HDFS installation and NameNode formatting
- Hive 3.1.3 + Derby metastore initialization (including the Guava jar fix)
- Kafka + Zookeeper startup
- Elasticsearch + Kibana configuration
- Running each pipeline component

Quick start (assumes all services running):

```bash
# 1. Copy and fill in credentials
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create Hive table
python hive/setup_hive.py

# 4. Start producer (CoinGecko)
python producer/coingecko_producer.py

# 5. Start Spark stream processor (separate terminal)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark/crypto_analysis.py

# 6. Start Hive sink (separate terminal)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark/kafka_to_hive.py

# 7. Start Elasticsearch indexer (separate terminal)
python dashboard/kafka_to_elastic.py
```

## Environment Variables

| Variable | Description |
|---|---|
| `COINGECKO_API_KEY` | CoinGecko demo API key |
| `ALPACA_API_KEY` | Alpaca Markets API key |
| `ALPACA_SECRET_KEY` | Alpaca Markets secret |
| `KAFKA_BROKER` | Kafka broker address (default: `localhost:9092`) |
| `HIVE_HOST` | Hive server host (default: `localhost`) |
| `HIVE_PORT` | HiveServer2 port (default: `10000`) |

## Tracked Coins

Bitcoin (BTC), Ethereum (ETH), Solana (SOL) — configurable via `COIN_IDS` in `config/settings.py`.

## Ports

| Service | Port |
|---|---|
| SSH | 22 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| HDFS NameNode UI | 9870 |
| HiveServer2 | 10000 |
| Kibana | 5601 |

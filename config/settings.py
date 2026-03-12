import os
from dotenv import load_dotenv

load_dotenv()

# CoinGecko API
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Kafka
KAFKA_BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_RAW_TOPIC      = "crypto-stream"
KAFKA_ANALYZED_TOPIC = "crypto-analyzed"

# Hive / HDFS
HIVE_HOST      = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT      = int(os.getenv("HIVE_PORT", 10000))
HIVE_DATABASE  = "crypto"
HIVE_TABLE     = "crypto_data"
HDFS_WAREHOUSE = "/user/hive/warehouse/crypto.db/crypto_data"

# Coins to track (CoinGecko IDs)
COIN_IDS = ["bitcoin", "ethereum", "solana"]

# Poll interval in seconds (free tier: 30 calls/min)
POLL_INTERVAL = 30

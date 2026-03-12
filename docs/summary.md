# Summary — Learning Experience & Insights

## 3.2.e — Summary of Learning Experience

### Overview
This project involved building a real-time crypto market trend analyzer using a full big data pipeline stack: CoinGecko API → Kafka → PySpark → Hive (HDFS) → Kibana.

---

### What We Built
- **Real-time data ingestion** from CoinGecko API (BTC, ETH, SOL) polled every 30 seconds
- **Kafka messaging layer** with dedicated topics (`crypto-stream`, `crypto-analyzed`)
- **PySpark streaming analysis** computing 5-minute rolling averages, volatility, and trend signals
- **Hive storage** in Parquet format on HDFS for structured querying
- **Kibana dashboard** for real-time visualization of price trends

---

### Challenges Faced

**1. Java Version Compatibility**
The biggest challenge was Hive 3.1.3's incompatibility with Java 11. The error `ClassCastException: AppClassLoader cannot be cast to URLClassLoader` took significant debugging time. The fix was installing Java 8 alongside Java 11 and explicitly setting `JAVA_HOME` in `hadoop-env.sh`. This taught us the importance of checking tool compatibility matrices before installation.

**2. Disk Space on Free Tier**
AWS t2.micro comes with 8 GB EBS by default, which was insufficient. Resizing to 30 GB (still free tier) and expanding the filesystem using `parted` and `xfs_growfs` was a non-trivial operation due to the GPT partition table format.

**3. Memory Constraints**
With only 1 GB RAM on t2.micro, running Hadoop + Hive + Kafka + Zookeeper simultaneously pushed memory to its limits. Adding a 2 GB swap file was essential to keep all services running.

**4. Cross-Team Coordination**
Working with two separate Git branches required careful coordination. A topic name mismatch (`crypto-stream` vs `crypto-raw`) was caught early through communication and resolved by updating the shared config file.

**5. Python Version Compatibility**
EC2 runs Python 3.9 while the code was written on Python 3.10+. The `dict | None` union type syntax caused a `TypeError` that required updating the code for backwards compatibility.

---

### Insights Gained

**Kafka as the backbone:** Kafka decoupled the producer and consumer completely. The producer could keep running even when the Spark job was stopped, and Spark could replay messages from any offset — this resilience is invaluable in production systems.

**PySpark's power for streaming:** Writing the same windowing and aggregation logic in plain Python would require managing state manually. PySpark's Structured Streaming handled watermarking, late data, and micro-batching automatically.

**Hive + Parquet is a great combo:** Storing analyzed data as Parquet in HDFS and exposing it through Hive gave us both efficient columnar storage and SQL-based access. Kibana could then visualize directly from Elasticsearch which consumed the same Kafka stream.

**Free tier is viable for learning:** Despite the memory and storage constraints, AWS free tier was sufficient for a class project with careful resource management (swap file, stopping unused services, 30 GB EBS).

**Importance of a shared config:** Centralizing all configuration in `config/settings.py` and committing it early prevented integration issues between teammates.

---

### What We Would Do Differently
- Use **Docker Compose** locally for development to avoid manual installation issues
- Use a larger instance (t2.medium) for smoother performance during testing
- Set up **Confluent Cloud** for Kafka to eliminate Zookeeper management overhead
- Add **schema validation** at the Kafka producer level using Avro or JSON Schema

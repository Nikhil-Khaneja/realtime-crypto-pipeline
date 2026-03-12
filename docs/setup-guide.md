# Setup Guide — Real-Time Crypto Trend Analyzer

## Environment
- **Cloud:** AWS EC2 (t2.micro, Amazon Linux 2023, 30 GB EBS)
- **Stack:** Java 8, Python 3.9, Hadoop 3.3.6, Hive 3.1.3, Kafka 3.5.1, Zookeeper

---

## Phase 1 — EC2 Setup

### Steps
1. Launched t2.micro instance with Amazon Linux 2023 on AWS
2. Created key pair (`crypto-pipeline-key.pem`) and secured with `chmod 400`
3. Configured security group with ports: 22, 9092, 2181, 9870, 10000, 5601
4. Resized EBS volume from 8 GB → 30 GB using AWS Console (free tier)
5. Added 2 GB swap file to compensate for t2.micro's 1 GB RAM

### Technical Difficulty
- **Default disk was 8 GB** — not enough to download Hadoop (697 MB tar.gz).
  Fix: Modified EBS volume to 30 GB and used `parted` + `xfs_growfs` to expand the partition.
- **`/tmp` was a RAM-based tmpfs (100% full)** — Hadoop couldn't write PID files.
  Fix: Set `HADOOP_PID_DIR=/opt/hadoop/tmp` in `hadoop-env.sh`.

### Best Practice
Always set EBS storage to 30 GB (free tier max) before launching — the default 8 GB is insufficient for big data tools.

---

## Phase 2 — Hadoop & HDFS Setup

### Steps
1. Installed Java 8 (Amazon Corretto): `sudo yum install java-1.8.0-amazon-corretto-devel`
2. Downloaded Hadoop 3.3.6 from Apache archive
3. Configured `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`
4. Formatted NameNode: `hdfs namenode -format`
5. Started HDFS: `start-dfs.sh`
6. Created HDFS directories: `/user/hive/warehouse`

### Technical Difficulty
- **Java 11 vs Java 8 conflict** — Hive 3.1.3 has a known ClassLoader incompatibility with Java 11.
  Fix: Installed Java 8 Corretto and updated `JAVA_HOME` in `hadoop-env.sh`.
  All processes (NameNode, DataNode, HiveServer2) now run on Java 8.
- **`growpart` failed on GPT partition table** — standard `growpart` tool didn't work.
  Fix: Used `parted` with `resizepart` command interactively to expand the partition.

### Best Practice
Set `JAVA_HOME` explicitly in `hadoop-env.sh` — relying on system default causes version conflicts when multiple Java versions are installed.

---

## Phase 2b — Hive Setup

### Steps
1. Downloaded Apache Hive 3.1.3
2. Fixed Guava jar conflict: replaced `guava-19.0.jar` with `guava-27.0-jre.jar` from Hadoop
3. Initialized Derby metastore: `schematool -dbType derby -initSchema`
4. Started HiveServer2: `nohup hiveserver2 &`
5. Created `crypto` database via beeline

### Technical Difficulty
- **Hive 3.1.3 + Java 11 incompatibility** — `ClassCastException: AppClassLoader cannot be cast to URLClassLoader`
  Fix: Used Java 8 for all Hive/Hadoop processes.
- **Guava version conflict** between Hadoop 3.3.6 and Hive 3.1.3
  Fix: Copied Hadoop's newer Guava jar into Hive's lib directory.

### Best Practice
Always check dependency version compatibility (Guava, SLF4J) when combining Hadoop and Hive. Remove duplicate jars to avoid classpath conflicts.

---

## Phase 3 — Kafka Producer

### Steps
1. Installed Kafka 3.5.1 (handled by Person B)
2. Created Kafka topics: `crypto-stream`, `crypto-analyzed`
3. Installed Python dependencies: `pip3 install requests kafka-python python-dotenv`
4. Cloned repo and configured `.env` with CoinGecko API key
5. Ran producer: `python3 producer/coingecko_producer.py`

### Technical Difficulty
- **Python 3.9 does not support `dict | None` union type hint syntax** (Python 3.10+ feature)
  Fix: Removed the return type annotation from the `preprocess()` function.
- **Kafka topic name mismatch** between Person A and Person B
  Fix: Coordinated via GitHub — updated `settings.py` to use agreed topic name `crypto-stream`.

### Best Practice
Define shared constants (topic names, schema) in a single `config/settings.py` and commit it to the shared repo early — prevents integration issues between teammates.

---

## Phase 5 — Hive Storage Verification

### Steps
1. Verified Parquet files written to HDFS by PySpark job
2. Queried Hive table using beeline to confirm data rows

### Best Practice
Use external Hive tables (not managed) pointing to HDFS paths — this way data persists even if the table is accidentally dropped.

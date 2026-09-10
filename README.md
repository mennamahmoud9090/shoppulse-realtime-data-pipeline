# ShopPulse Global: Real-Time E-Commerce Data Pipeline

An enterprise-grade, fault-tolerant real-time data ingestion and processing platform built to replace traditional high-latency batch reporting. This pipeline streams live customer transactions through **Apache Kafka**, processes and enriches micro-batches using **Apache Spark Structured Streaming**, persists columnar data into **HDFS (Parquet)**, and exposes it for real-time analytics via an **Apache Hive External Lakehouse**.

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-streaming-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark)
![Hive](https://img.shields.io/badge/Apache%20Hive-lakehouse-FDEE21?logo=apachehive)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Data Schema](#data-schema)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Component Details](#component-details)
- [Analytical Queries](#analytical-queries)
- [Fault Tolerance & Recovery](#fault-tolerance--recovery)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture Overview

```text
[ Python Producer ] ---> ( Apache Kafka: topic1_logs )
                                |
                                v
                   [ Spark Structured Streaming ]
                    (Enriches & Computes Totals)
                                |
                                v
                     [ HDFS Storage (Parquet) ]
                                |
                                v
                    [ Apache Hive Metastore ]
                                |
                                v
                     [ OLAP Analytics (SQL) ]
```

**Flow summary:** a Python producer emits synthetic order events to Kafka → Spark Structured Streaming consumes, parses, and enriches each micro-batch → enriched records land in HDFS as Parquet → an external Hive table maps to that warehouse path → analysts query it with standard SQL.

---

## Project Structure

```text
kafka_spark_pipeline/
├── producer/
│   └── generate_orders.py     # Synthetic order stream generator
├── spark/
│   └── spark_streaming.py     # Structured Streaming job
├── hive/
│   └── create_table.sql       # External table + OLAP queries
├── scripts/
│   ├── start_pipeline.sh      # Orchestrated startup
│   └── stop_pipeline.sh       # Graceful shutdown
└── README.md
```

---

## Data Schema

| Field Name      | Source Type | Target Spark Type | Description / Generation Rule |
|------------------|-------------|--------------------|--------------------------------|
| `order_id`       | Integer     | `IntegerType`      | Monotonically increasing unique order identifier (starting at 1001). |
| `customer_id`    | Integer     | `IntegerType`      | Random integer representing registered accounts (100–500). |
| `product_id`     | Integer     | `IntegerType`      | Random integer catalog reference (1–50). |
| `quantity`       | Integer     | `IntegerType`      | Purchased item volume between 1 and 10. |
| `price`          | Float       | `DoubleType`       | Unit price between $5.00 and $500.00, rounded to 2 decimal places. |
| `order_time`     | String      | `TimestampType`    | Ingestion timestamp (`YYYY-MM-DD HH:MM:SS`), parsed to native timestamp. |
| `total_amount`   | Derived     | `DoubleType`       | Computed column: `quantity × price`. |

---

## Prerequisites

Ensure your local or cluster environment has the following services installed and running:

- **Apache Hadoop / HDFS** — NameNode & DataNode active
- **Apache Kafka** — with ZooKeeper or KRaft broker running at `localhost:9092`
- **Apache Spark (v3.x+)** — with PySpark configured
- **Apache Hive / Metastore service**

---

## Quick Start

### 1. Launch the Pipeline

Use the automated orchestration script to check Hadoop/Kafka health, set up HDFS directories, initialize Hive tables, and start background workers:

```bash
bash scripts/start_pipeline.sh
```

### 2. Stop the Pipeline Gracefully

Terminate the Python producer and Spark streaming jobs safely while preserving HDFS data, Kafka offsets, and Spark checkpoint recovery states:

```bash
bash scripts/stop_pipeline.sh
```

---

## Component Details

### Task 1 — Synthetic Stream Generation
`producer/generate_orders.py`

Continuously simulates live order transactions and pushes JSON payloads asynchronously to the Kafka topic.

| Setting  | Value |
|----------|-------|
| Broker   | `localhost:9092` |
| Topic    | `topic1_logs` |
| Interval | Configurable, 1.0–2.0 seconds between events |

### Task 2 — Spark Structured Streaming
`spark/spark_streaming.py`

Consumes from Kafka, unpacks JSON records using an explicit schema, parses timestamps, and computes the derived metric:

```
total_amount = quantity × price
```

| Setting            | Value |
|---------------------|-------|
| Storage Path        | `/user/hive/warehouse/ecommerce_dw.db/streaming_orders` |
| Checkpoint Path      | `/user/spark/checkpoints/streaming_orders` |
| Trigger Interval    | 10 seconds |
| Output Mode / Format | Append, Parquet |

### Task 3 — Hive Lakehouse Integration & OLAP
`hive/create_table.sql`

Maps an external Hive table directly to the HDFS Parquet warehouse path and enables production analytical queries.

---

## Analytical Queries

The `hive/create_table.sql` script includes the following core analytical metrics:

- Total number of processed orders
- Total gross revenue
- Average Order Value (AOV)
- Total sales broken down by product ID
- Total expenditure per customer ID
- Top 5 selling products ranked by gross revenue

---

## Fault Tolerance & Recovery

| Scenario                | Behavior |
|--------------------------|----------|
| **Producer failure**     | Stopping and restarting the Python producer allows Spark to seamlessly resume data ingestion without service crashes. |
| **Spark checkpoint recovery** | Terminating the Spark streaming application and restarting it triggers exact offset recovery from the checkpoint directory (`/user/spark/checkpoints/streaming_orders`), ensuring zero data loss and uncorrupted Parquet part-files. |

---

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License — see the `LICENSE` file for details.

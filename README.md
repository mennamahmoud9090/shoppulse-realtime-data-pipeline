ShopPulse Global: Real-Time E-Commerce Data PipelineAn enterprise-grade, fault-tolerant real-time data ingestion and processing platform built to replace traditional high-latency batch reporting. This pipeline streams live customer transactions through Apache Kafka, processes and enriches micro-batches using Apache Spark Structured Streaming, persists columnar data into HDFS (Parquet), and exposes it for real-time analytics via an Apache Hive External Lakehouse.🏗️ Architecture OverviewPlaintext[ Python Producer ] ---> ( Apache Kafka: topic1_logs )
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
Planned Directory StructurePlaintextkafka_spark_pipeline/
├── producer/
│   └── generate_orders.py
├── spark/
│   └── spark_streaming.py
├── hive/
│   └── create_table.sql
├── scripts/
│   ├── start_pipeline.sh
│   └── stop_pipeline.sh
└── README.md
📊 Data Schema SpecificationField NameSource TypeTarget Spark TypeDescription / Generation Ruleorder_idIntegerIntegerTypeMonotonically increasing unique order identifier (starting at 1001).customer_idIntegerIntegerTypeRandom integer representing registered accounts ($100$ to $500$).product_idIntegerIntegerTypeRandom integer catalog reference ($1$ to $50$).quantityIntegerIntegerTypePurchased item volume between $1$ and $10$.priceFloatDoubleTypeUnit price between $\$5.00$ and $\$500.00$ rounded to 2 decimal places.order_timeStringTimestampTypeIngestion timestamp (YYYY-MM-DD HH:MM:SS), parsed to native timestamp.total_amountDerivedDoubleTypeComputed column: $\text{quantity} \times \text{price}$.⚙️ Prerequisites & Environment SetupEnsure your local or cluster environment has the following services installed and running:Apache Hadoop / HDFS (NameNode & DataNode active)Apache Kafka (with ZooKeeper or KRaft broker running at localhost:9092)Apache Spark (v3.x+) with PySpark configuredApache Hive / Metastore service🚀 Quick Start & Lifecycle Management1. Launch the PipelineUse the automated orchestration script to check Hadoop/Kafka health, set up HDFS directories, initialize Hive tables, and start background workers:Bashbash scripts/start_pipeline.sh
2. Stop the Pipeline GracefullyTerminate the Python producer and Spark streaming jobs safely while preserving HDFS data, Kafka offsets, and Spark checkpoint recovery states:Bashbash scripts/stop_pipeline.sh
🛠️ Component DetailsTask 1: Synthetic Stream Generation (producer/generate_orders.py)Continuously simulates live order transactions and pushes JSON payloads asynchronously to the Kafka topic topic1_logs.Broker: localhost:9092Topic: topic1_logsInterval: Configurable (1.0 to 2.0 seconds between events)Task 2: Spark Structured Streaming (spark/spark_streaming.py)Consumes from Kafka, unpacks JSON records using an explicit schema, parses timestamps, and computes the derived metric:$$\text{total\_amount} = \text{quantity} \times \text{price}$$Storage Path: /user/hive/warehouse/ecommerce_dw.db/streaming_ordersCheckpoint Path: /user/spark/checkpoints/streaming_ordersTrigger Interval: 10 seconds (Append Mode, Parquet format)Task 3: Hive Lakehouse Integration & OLAP (hive/create_table.sql)Maps an external Hive table directly to the HDFS Parquet warehouse path and enables production analytical queries.📈 Analytical OLAP QueriesThe hive/create_table.sql script includes the following core analytical metrics:Total Number of Processed OrdersTotal Gross RevenueAverage Order Value (AOV)Total Sales Broken Down by Product IDTotal Expenditure per Customer IDTop 5 Selling Products Ranked by Gross Revenue🛡️ Fault Tolerance & Recovery VerificationProducer Failure: Stopping and restarting the Python producer allows Spark to seamlessly resume data ingestion without service crashes.Spark Checkpoint Recovery: Terminating the Spark streaming application and restarting it triggers exact offset recovery from the checkpoint directory (/user/spark/checkpoints/streaming_orders), ensuring zero data loss and uncorrupted Parquet part-files.

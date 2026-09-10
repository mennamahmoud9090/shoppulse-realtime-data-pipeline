#!/bin/bash

KAFKA_TOPIC="topic1_logs"
HDFS_DATA_PATH="/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
HDFS_CHECKPOINT_PATH="/user/spark/checkpoints/streaming_orders"
LOG_DIR="logs"

mkdir -p $LOG_DIR

# 1. Check Hadoop
hdfs dfsadmin -report > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Hadoop cluster is not running."
    exit 1
fi

# 2. Check/Create Kafka Topic
if kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "$KAFKA_TOPIC"; then
    echo "Topic exists."
else
    kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC
fi

# 3. Create HDFS Directories
hdfs dfs -mkdir -p $HDFS_DATA_PATH
hdfs dfs -mkdir -p $HDFS_CHECKPOINT_PATH

# 4. Initialize Hive Table DDL
hive -f hive/create_table.sql

# 5. Start Python Producer
nohup python3 producer/generate_orders.py > "$LOG_DIR/producer.log" 2>&1 &
echo $! > "$LOG_DIR/producer.pid"
echo "Producer started with PID $(cat "$LOG_DIR/producer.pid")."

# 6. Start Spark Streaming (Using the exact working package version 3.1.2)
nohup spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    spark/spark_streaming.py > "$LOG_DIR/spark_streaming.log" 2>&1 &
echo $! > "$LOG_DIR/spark_streaming.pid"
echo "Spark streaming started with PID $(cat "$LOG_DIR/spark_streaming.pid")."

echo "Pipeline started successfully."

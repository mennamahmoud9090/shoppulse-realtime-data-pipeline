#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "topic1_logs"

# 1. Create Spark Session
spark = (
    SparkSession.builder
    .appName("ShopPulse_RealTime_Ingestion")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Define JSON Schema
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_time", StringType(), True)
])

# 3. Read Kafka Stream
raw_orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

# 4. Unpack JSON Payload and Format Columns
orders = (
    raw_orders
    .select(from_json(col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withColumn("order_timestamp", col("order_time").cast("timestamp"))
    .withColumn("total_amount", expr("CAST(quantity * price AS DOUBLE)"))
    .drop("order_time")
)

# 5. Write Stream to HDFS as Parquet
hdfs_data_path = "/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
hdfs_checkpoint_path = "/user/spark/checkpoints/streaming_orders"

query = (
    orders.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", hdfs_data_path)
    .option("checkpointLocation", hdfs_checkpoint_path)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Spark Streaming Job started on topic '{TOPIC_NAME}'. "
      f"Processing micro-batches to HDFS Parquet...")
query.awaitTermination()


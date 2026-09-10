#!/usr/bin/env python3
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

TOPIC_NAME = "topic1_logs"
order_id = 1001

print(f"Starting order stream to Kafka topic: {TOPIC_NAME}...")

while True:
    order = {
        "order_id": order_id,
        "customer_id": random.randint(100, 500),
        "product_id": random.randint(1, 50),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(5.0, 500.0), 2),
        "order_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send(TOPIC_NAME, key=order["order_id"], value=order)
    producer.flush()

    print(f"[PRODUCED] Order ID: {order['order_id']} | Total: ${order['quantity'] * order['price']:.2f}")

    order_id += 1
    time.sleep(random.uniform(1.0, 2.0))


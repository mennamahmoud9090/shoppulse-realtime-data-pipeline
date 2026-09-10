#!/bin/bash

LOG_DIR="logs"

# Stop Producer
if [ -f "$LOG_DIR/producer.pid" ]; then
    PID=$(cat "$LOG_DIR/producer.pid")
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID 2>/dev/null
        echo "Producer stopped."
    else
        echo "Producer was not running."
    fi
    rm -f "$LOG_DIR/producer.pid"
fi

# Stop Spark Streaming (Killing both the submit wrapper and potential driver descendants)
if [ -f "$LOG_DIR/spark_streaming.pid" ]; then
    PID=$(cat "$LOG_DIR/spark_streaming.pid")
    if ps -p $PID > /dev/null 2>&1; then
        # Kill the process and any child processes spawned by it
        pkill -P $PID 2>/dev/null
        kill $PID 2>/dev/null
        echo "Spark streaming stopped."
    else
        echo "Spark streaming was not running."
    fi
    rm -f "$LOG_DIR/spark_streaming.pid"
fi

echo "Pipeline stopped successfully."

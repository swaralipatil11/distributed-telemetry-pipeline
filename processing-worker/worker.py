import os
import json
import time
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

DB_HOST = os.getenv("DB_HOST", "localhost")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

print(f"[INFO] Initializing worker network mapping -> DB: {DB_HOST}, Kafka: {KAFKA_BROKER}")

# 1. Connect to the TimescaleDB Service
try:
    db_conn = psycopg2.connect(
        host=DB_HOST,
        database="telemetry_db",
        user="postgres",
        password="password",
        port="5432"
    )
    cursor = db_conn.cursor()
    print("[SUCCESS] Connected to TimescaleDB.")
except Exception as e:
    print(f"[ERROR] Database connection failed: {e}")
    exit(1)

# 2. Connect to Kafka with a Graceful Retry Loop
consumer = None
print("[INFO] Attempting to connect to Kafka Broker...")
for attempt in range(1, 11):
    try:
        consumer = KafkaConsumer(
            'raw-telemetry',
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(3, 5, 0)  # Forces compatibility handshake
        )
        print("[SUCCESS] Kafka Consumer listening on 'raw-telemetry' topic!")
        break
    except NoBrokersAvailable:
        print(f"[RETRY {attempt}/10] Kafka isn't ready yet. Retrying in 5 seconds...")
        time.sleep(5)

if not consumer:
    print("[FATAL] Could not connect to Kafka Broker after multiple attempts.")
    exit(1)

# 3. Continuous Processing Loop
try:
    for message in consumer:
        data = message.value
        print(f"[PROCESSING] Received event from machine: {data['machine_id']}")
        
        formatted_time = datetime.fromtimestamp(data['timestamp'])
        
        if data['status'] == "CRITICAL":
            print(f"⚠️  [ALERT] Instance {data['machine_id']} reports CRITICAL status!")

        cursor.execute(
            "INSERT INTO metrics (machine_id, cpu_utilization, memory_utilization, status, timestamp) VALUES (%s, %s, %s, %s, %s)",
            (data['machine_id'], data['cpu_utilization'], data['memory_utilization'], data['status'], formatted_time)
        )
        db_conn.commit()

except KeyboardInterrupt:
    print("\nShutting down pipeline worker safely...")
finally:
    if cursor: cursor.close()
    if db_conn: db_conn.close()
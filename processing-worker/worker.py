import os
import json
import time
import psycopg2
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "telemetry_db")
DB_PORT = os.getenv("DB_PORT", "5432")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

print(f"[INFO] Initializing worker network mapping -> DB: {DB_HOST}:{DB_PORT}/{DB_NAME}, Kafka: {KAFKA_BROKER}")

# 1. Connect to the TimescaleDB Service
try:
    db_conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
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
            api_version=(3, 5, 0),  # Forces compatibility handshake
            group_id='telemetry-worker-group',
            enable_auto_commit=True
        )
        print("[SUCCESS] Kafka Consumer listening on 'raw-telemetry' topic (Consumer Group: telemetry-worker-group)!")
        break
    except NoBrokersAvailable:
        sleep_time = min(30, 2 ** attempt)  # Exponential backoff retry logic: 2s, 4s, 8s, 16s...
        print(f"[RETRY {attempt}/10] Kafka isn't ready yet. Retrying in {sleep_time} seconds...")
        time.sleep(sleep_time)

if not consumer:
    print("[FATAL] Could not connect to Kafka Broker after multiple attempts.")
    exit(1)

# 3. Continuous Processing Loop
try:
    for message in consumer:
        data = message.value
        print(f"[PROCESSING] Received event from machine: {data['machine_id']}")
        
        formatted_time = datetime.fromtimestamp(data['timestamp'], tz=timezone.utc)
        
        # Stream-based rule anomaly detection logic
        is_anomaly = False
        reasons = []
        if data.get('cpu_utilization', 0.0) > 90.0:
            is_anomaly = True
            reasons.append(f"CPU utilization ({data['cpu_utilization']}%) exceeds threshold")
        if data.get('memory_utilization', 0.0) > 90.0:
            is_anomaly = True
            reasons.append(f"Memory utilization ({data['memory_utilization']}%) exceeds threshold")
        if data.get('status') == "CRITICAL":
            is_anomaly = True
            reasons.append("Critical status flag reported")

        if is_anomaly:
            print(f"⚠️  [ANOMALY DETECTED] Instance {data['machine_id']}: {', '.join(reasons)}")

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
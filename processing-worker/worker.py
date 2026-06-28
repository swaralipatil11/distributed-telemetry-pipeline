import os
import json
import time
import sys
import signal
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "telemetry_db")
DB_PORT = os.getenv("DB_PORT", "5432")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
HEALTH_FILE = "/tmp/healthy"

print(f"[INFO] Initializing worker network mapping -> DB: {DB_HOST}:{DB_PORT}/{DB_NAME}, Kafka: {KAFKA_BROKER}")

# State variables
running = True
db_conn = None
cursor = None
consumer = None

def handle_shutdown(signum, frame):
    global running
    print(f"[INFO] Received termination signal ({signum}). Initiating graceful shutdown...")
    running = False

# Setup signal handlers for Kubernetes graceful lifecycle management
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

def connect_db():
    print("[INFO] Attempting to connect to TimescaleDB...")
    for attempt in range(1, 11):
        if not running:
            break
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            cur = conn.cursor()
            print("[SUCCESS] Connected to TimescaleDB.")
            
            # Bootstrapping migration: ensure metrics table and hypertable exist
            print("[INFO] Running database schema migrations...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp TIMESTAMPTZ NOT NULL,
                    machine_id VARCHAR(50) NOT NULL,
                    cpu_utilization DOUBLE PRECISION NOT NULL,
                    memory_utilization DOUBLE PRECISION NOT NULL,
                    status VARCHAR(20) NOT NULL
                );
            """)
            cur.execute("SELECT create_hypertable('metrics', 'timestamp', if_not_exists => TRUE);")
            conn.commit()
            print("[SUCCESS] Database schema migrations completed successfully.")
            
            return conn, cur
        except Exception as e:
            sleep_time = min(30, 2 ** attempt)
            print(f"[RETRY {attempt}/10] Database connection/migration failed: {e}. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    print("[FATAL] Could not connect to TimescaleDB after multiple attempts.")
    sys.exit(1)

def connect_kafka():
    print("[INFO] Attempting to connect to Kafka Broker...")
    for attempt in range(1, 11):
        if not running:
            break
        try:
            cons = KafkaConsumer(
                'raw-telemetry',
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                api_version=(3, 5, 0),  # Forces compatibility handshake
                group_id='telemetry-worker-group',
                enable_auto_commit=False  # Disable auto-commit for at-least-once delivery
            )
            print("[SUCCESS] Kafka Consumer listening on 'raw-telemetry' topic (Consumer Group: telemetry-worker-group)!")
            return cons
        except NoBrokersAvailable:
            sleep_time = min(30, 2 ** attempt)
            print(f"[RETRY {attempt}/10] Kafka isn't ready yet. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    print("[FATAL] Could not connect to Kafka Broker after multiple attempts.")
    sys.exit(1)

def touch_health_file():
    try:
        with open(HEALTH_FILE, 'w') as f:
            f.write("OK")
    except Exception as e:
        print(f"[WARN] Failed to update health check file: {e}")

# 1. Establish Initial Database Connection
db_conn, cursor = connect_db()

# 2. Establish Initial Kafka Consumer Connection
consumer = connect_kafka()

# Touch health file to mark startup success
touch_health_file()

# 3. Resilient Continuous Processing Loop
try:
    while running:
        try:
            # Poll Kafka (wait up to 1000ms for incoming events)
            msg_pack = consumer.poll(timeout_ms=1000)
            
            if not msg_pack:
                touch_health_file()
                continue
            
            metrics_batch = []
            for tp, messages in msg_pack.items():
                for message in messages:
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

                    metrics_batch.append((
                        formatted_time,
                        data['machine_id'],
                        data['cpu_utilization'],
                        data['memory_utilization'],
                        data['status']
                    ))

            # Perform Bulk Insert and Manual Offset Committing
            if metrics_batch:
                db_success = False
                while not db_success and running:
                    try:
                        # Batch insert using psycopg2 execute_values (highly optimized)
                        insert_query = """
                            INSERT INTO metrics (timestamp, machine_id, cpu_utilization, memory_utilization, status)
                            VALUES %s
                        """
                        psycopg2.extras.execute_values(cursor, insert_query, metrics_batch)
                        db_conn.commit()
                        db_success = True
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                        print(f"[ERROR] Database connection lost during insert: {db_err}. Reconnecting...")
                        try:
                            if cursor: cursor.close()
                        except Exception:
                            pass
                        try:
                            if db_conn: db_conn.close()
                        except Exception:
                            pass
                        db_conn, cursor = connect_db()
                    except Exception as other_err:
                        print(f"[ERROR] Database insertion failed with logical error: {other_err}. Rolling back batch...")
                        try:
                            db_conn.rollback()
                        except Exception:
                            pass
                        # Break to avoid infinite loop on malformed data / schema issues.
                        break
                
                if db_success:
                    # Commit offsets to Kafka ONLY after database transaction is confirmed complete
                    consumer.commit()
                    print(f"[SUCCESS] Successfully processed and committed batch of {len(metrics_batch)} events.")
            
            # Touch health file on successful loop cycle
            touch_health_file()
            
        except Exception as err:
            print(f"[ERROR] Error in main consumer processing loop: {err}")
            time.sleep(1)

except KeyboardInterrupt:
    print("\n[INFO] KeyboardInterrupt caught. Shutting down pipeline worker safely...")
finally:
    print("[INFO] Cleaning up resources...")
    if cursor:
        try: cursor.close()
        except Exception: pass
    if db_conn:
        try: db_conn.close()
        except Exception: pass
    if consumer:
        try: consumer.close()
        except Exception: pass
    
    # Remove healthy file on exit
    if os.path.exists(HEALTH_FILE):
        try: os.remove(HEALTH_FILE)
        except Exception: pass
        
    print("[SUCCESS] Resilient shutdown complete.")
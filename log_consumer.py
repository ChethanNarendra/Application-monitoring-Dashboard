from kafka import KafkaConsumer
import json
import psycopg2
import uuid

# PostgreSQL setup
conn = psycopg2.connect(
    host="localhost",
    database="logsdb",
    user="appuser",
    password="appuserpass"
)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ,
        endpoint TEXT,
        status INTEGER
    )
''')
conn.commit()

# Kafka consumer
consumer = KafkaConsumer(
    'app-logs',
    bootstrap_servers='localhost:9092',
    group_id=f'log-saver-{uuid.uuid4()}',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🚀 Listening to Kafka topic: app-logs")
for message in consumer:
    log = message.value
    print(f"⬇️  Received: {log}")
    cursor.execute(
        "INSERT INTO logs (timestamp, endpoint, status) VALUES (%s, %s, %s)",
        (log['timestamp'], log['endpoint'], log['status'])
    )
    conn.commit()

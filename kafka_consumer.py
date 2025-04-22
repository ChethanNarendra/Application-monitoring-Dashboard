from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="logsdb",
    user="appuser",
    password="appuserpass"
)
cursor = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'app-logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for logs...")

for message in consumer:
    log = message.value
    print(f"Received log: {log}")
    cursor.execute(
        'INSERT INTO logs (timestamp, endpoint, status) VALUES (%s, %s, %s)',
        (log['timestamp'], log['endpoint'], log['status'])
    )
    conn.commit()

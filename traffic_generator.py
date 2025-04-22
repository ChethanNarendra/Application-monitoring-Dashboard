import requests
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import json

ENDPOINTS = ["/users", "/logs", "/login", "/products", "/events", "/settings", "/status"]
BASE_URL = "http://localhost:3000"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_log(endpoint, response_status):
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": endpoint,
        "status": response_status
    }

while True:
    endpoint = random.choice(ENDPOINTS)
    url = BASE_URL + endpoint
    try:
        response = requests.get(url)
        log = generate_log(endpoint, response.status_code)
        producer.send("app-logs", log)
        print(f"Sent to Kafka: {log}")
    except Exception as e:
        print({"error": str(e), "endpoint": endpoint})
    time.sleep(random.uniform(0.5, 2))

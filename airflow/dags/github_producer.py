#!/usr/bin/env python3

import os
import requests
import time
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = "kafka:9092"
GITHUB_TOKEN = os.environ.get("TOKEN")
TOPIC = "git_logs"

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP}")
print(f"Using topic: {TOPIC}")
print(f"GitHub token present: {bool(GITHUB_TOKEN)}")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=10000,
        retries=3
    )
    print("Kafka producer created successfully")
except Exception as e:
    print(f"Failed to create Kafka producer: {e}")
    exit(1)

HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
last_id = None

def fetch_github_events():
    global last_id
    try:
        print("Fetching GitHub events...")
        r = requests.get("https://api.github.com/events", headers=HEADERS, timeout=10)
        print(f"HTTP Status: {r.status_code}")
        
        if r.status_code != 200:
            print(f"Error: HTTP {r.status_code}")
            return
        
        events = r.json()
        print(f"Received {len(events)} events")
        
        if not isinstance(events, list):
            print("Error: Expected list of events")
            return

        new_events = 0
        for e in events:
            if e.get("id") == last_id:
                break
            producer.send(TOPIC, value=e)
            new_events += 1

        producer.flush()
        print(f"Sent {new_events} new events to Kafka")

        if events:
            last_id = events[0].get("id")
            print(f"Last event ID: {last_id}")

    except Exception as e:
        print(f"Error fetching events: {e}")

def run_loop(interval=60):
    while True:
        fetch_github_events()
        time.sleep(interval)

if __name__ == "__main__":
    run_loop()
import time

from kafka import KafkaProducer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

def create_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            print("Producer connected successfully")
            return producer
        except Exception as e:
            print(f"Retry {i+1}/{retries}: Error connecting to Kafka: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka after retries")

producer = create_producer()

def send_to_kafka(topic: str, message: dict):
    producer.send(topic, value=message)
    producer.flush()

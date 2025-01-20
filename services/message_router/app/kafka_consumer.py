import time

from kafka import KafkaConsumer
import pika
import os
import json

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'topic_orders')

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'queue_orders')

def create_kafka_consumer():
    retries = 5
    for i in range(retries):
        try:
            print(f"Connecting to Kafka broker: {KAFKA_BROKER}")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='message_router_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
            print(f"Connected to Kafka broker. Listening to topic: {KAFKA_TOPIC}")
            return consumer
        except Exception as e:
            print(f"Retry {i+1}/{retries}: Error connecting to Kafka: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka after retries")


def create_rabbitmq_connection():
    retries = 5
    for i in range(retries):
        try:
            print(f"Connecting to RabbitMQ host: {RABBITMQ_HOST}")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            print("Successfully connected to RabbitMQ")
            return connection, channel
        except Exception as e:
            print(f"Retry {i+1}/{retries}: Failed to connect to RabbitMQ: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to RabbitMQ after retries")

def main():
    print("Starting kafka_consumer.py...")

    consumer = create_kafka_consumer()
    connection, channel = create_rabbitmq_connection()


    try:
        print(f"Declaring RabbitMQ queue: {RABBITMQ_QUEUE}")
        channel.queue_declare(queue=RABBITMQ_QUEUE)
        print(f"Queue declared: {RABBITMQ_QUEUE}")
    except Exception as e:
        print(f"Failed to declare RabbitMQ queue: {e}")
        consumer.close()
        connection.close()
        return

    print(f'Listening to Kafka topic: {KAFKA_TOPIC} and routing to RabbitMQ queue: {RABBITMQ_QUEUE}')

    try:
        for message in consumer:
            print(f"Received message from Kafka: {message.value}")
            try:
                channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(message.value),
                )
                print(f"Sent message to RabbitMQ queue: {RABBITMQ_QUEUE}")
            except Exception as e:
                print(f"Failed to send message to RabbitMQ: {e}")
    except Exception as e:
        print(f"Error while consuming Kafka messages: {e}")
    finally:
        print("Closing Kafka consumer and RabbitMQ connection...")
        consumer.close()
        connection.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()

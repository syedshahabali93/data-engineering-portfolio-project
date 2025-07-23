from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid

faker = Faker()

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Make sure this matches your Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)


# Function to generate sample data
def generate_sample_data():
    return {
        'id': str(uuid.uuid4()),  # Use uuid from Python's built-in module for structured UUIDs
        'name': faker.name(),
        'email': faker.email(),
        'timestamp': faker.iso8601(),
        'amount': round(random.uniform(10, 1000), 2)
    }


topic = 'sample-data-topic'
print("Producing messages...")

# Produce 10 messages to the Kafka topic
for _ in range(10):
    data = generate_sample_data()

    # Send data asynchronously and handle success/failure
    future = producer.send(topic, value=data)

    # Optional: Add callback and error handling
    future.add_callback(lambda record_metadata: print(
        f"Sent to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}"))
    future.add_errback(lambda e: print(f"Error sending message: {e}"))

    print(f"Sent: {data}")
    time.sleep(1)  # Sleep for 1 second before sending the next message

# Ensure all messages are flushed to Kafka
producer.flush()
print("All messages sent.")

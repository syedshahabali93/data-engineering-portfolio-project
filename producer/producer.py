from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid

faker = Faker()

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_sample_data():
    return {
        'id': str(uuid.uuid4()),
        'name': faker.name(),
        'email': faker.email(),
        'timestamp': faker.iso8601(),
        'amount': round(random.uniform(10, 1000), 2)
    }


topic = 'sample-data-topic'
print("Producing messages...")

for _ in range(10):
    data = generate_sample_data()

    future = producer.send(topic, value=data)

    future.add_callback(lambda record_metadata: print(
        f"Sent to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}"))
    future.add_errback(lambda e: print(f"Error sending message: {e}"))

    print(f"Sent: {data}")
    time.sleep(1)

producer.flush()
print("All messages sent.")

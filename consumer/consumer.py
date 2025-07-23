from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

from elasticsearch import Elasticsearch
from datetime import datetime
from collections import defaultdict
import logging
import json
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "sample-data-topic"

def ensure_kafka_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        logger.info(f"Kafka topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        logger.info(f"Kafka topic '{topic_name}' already exists.")
    except KafkaError as e:
        logger.warning(f"Kafka topic check failed: {e}")

ensure_kafka_topic(TOPIC_NAME)

es = Elasticsearch(hosts=["http://elasticsearch:9200"])

consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    bootstrap_servers=[KAFKA_BROKER]
)

aggregates = defaultdict(float)
last_agg_flush = time.time()
AGG_FLUSH_INTERVAL = 30

def is_valid(data):
    required_fields = ['email', 'amount']

    for field in required_fields:
        if field not in data or data[field] in [None, '', 'null']:
            logger.warning(f"Missing required field: {field} in {data}")
            return False

    if not isinstance(data['email'], str) or '@' not in data['email']:
        logger.warning(f"Invalid email format: {data['email']}")
        return False

    try:
        amount = float(data['amount'])
        if amount < 0:
            logger.warning(f"Negative amount not allowed: {amount}")
            return False
    except (TypeError, ValueError):
        logger.warning(f"Amount is not numeric: {data['amount']}")
        return False

    return True

logger.info("Consumer started. Waiting for messages...")

try:
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Consumed message: {data}")

            if not is_valid(data):
                continue

            data['name'] = data.get('name', '').strip().lower()
            data['email'] = data['email'].strip().lower()
            data['amount'] = float(data['amount'])
            data['ingested_at'] = datetime.fromtimestamp(message.timestamp / 1000).isoformat()

            es.index(index="sample-data-index", document=data)
            logger.info("Sent to Elasticsearch.")

            aggregates[data['email']] += data['amount']

            if time.time() - last_agg_flush > AGG_FLUSH_INTERVAL:
                for email, total_amount in aggregates.items():
                    agg_doc = {
                        "email": email,
                        "total_amount": round(total_amount, 2),
                        "aggregated_at": datetime.utcnow().isoformat()
                    }
                    es.index(index="aggregated-amounts-index", document=agg_doc)
                    logger.info(f"Aggregated: {agg_doc}")
                aggregates.clear()
                last_agg_flush = time.time()

        except Exception as msg_error:
            logger.warning(f"Failed to process message: {msg_error}")

except Exception as e:
    logger.error(f"Consumer error: {e}")
finally:
    consumer.close()
    logger.info("Shutting down consumer...")

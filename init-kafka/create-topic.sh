#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
while ! nc -z kafka 9092; do
  sleep 1
done

echo "Creating topic sample-data-topic..."
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic sample-data-topic \
  --partitions 1 \
  --replication-factor 1

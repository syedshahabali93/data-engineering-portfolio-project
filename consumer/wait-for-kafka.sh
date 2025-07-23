#!/bin/bash
set -e

host="$1"
port="$2"

echo "Waiting for Kafka at $host:$port..."

while ! nc -z "$host" "$port"; do
  sleep 1
done

echo "Kafka is up!"
#exec "${@:3}"



shift 2

echo "➡️  Running command: $@"

exec "$@"

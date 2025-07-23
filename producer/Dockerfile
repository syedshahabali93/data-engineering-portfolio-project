FROM python:3.9-slim

WORKDIR /app

COPY producer.py .
COPY wait-for-kafka.sh .

RUN apt-get update && apt-get install -y netcat-openbsd \
    && pip install kafka-python faker \
    && chmod +x wait-for-kafka.sh

CMD ["./wait-for-kafka.sh", "kafka", "9092", "python", "producer.py"]

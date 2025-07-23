FROM python:3.9-slim

WORKDIR /app

COPY consumer.py .
COPY wait-for-kafka.sh .

RUN apt-get update && apt-get install -y netcat-openbsd \
    && pip install kafka-python elasticsearch==8.12.0 \
    && chmod +x wait-for-kafka.sh

CMD ["./wait-for-kafka.sh", "kafka", "9092", "python", "consumer.py"]

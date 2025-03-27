import json
import csv
from kafka import KafkaProducer
import config
from loguru import logger

def json_serializer(data):
    """Convert a Python object to JSON bytes."""
    return json.dumps(data).encode('utf-8')

def key_serializer(key):
    """Encode the key as bytes."""
    return str(key).encode('utf-8')

def produce_to_kafka(csv_file_path, topic):
    """
    Reads a CSV file and sends each row as a message to the specified Kafka topic.
    Uses "Airport ID" as the message key if available.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKER,
            value_serializer=json_serializer,
            key_serializer=key_serializer
        )
    except Exception as e:
        logger.error("Could not create Kafka producer: {}", e)
        return

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            records = list(reader)
    except Exception as e:
        logger.error("Error reading CSV file {}: {}", csv_file_path, e)
        return

    if not records:
        logger.warning("CSV file {} is empty.", csv_file_path)
        return

    total_records = len(records)
    if total_records > config.MAX_RECORDS:
        logger.warning(
            "Number of records ({}) exceeds the maximum limit ({}). "
            "Using only the first {} records.",
            total_records, config.MAX_RECORDS, config.MAX_RECORDS
        )
        records = records[:config.MAX_RECORDS]

    logger.info("Sending {} records from file '{}' to Kafka topic '{}'.",
                len(records), csv_file_path, topic)

    futures = []
    for row in records:
        # Use "Airport ID" as the key if available; otherwise, default to "unknown"
        message_key = row.get("Airport ID", "unknown")
        try:
            future = producer.send(topic, key=message_key, value=row)
            futures.append(future)
        except Exception as e:
            logger.error("Failed to send message with key {}: {}", message_key, e)

    # Wait for all send operations to complete
    for future in futures:
        try:
            future.get(timeout=10)
        except Exception as e:
            logger.error("Error waiting for message confirmation: {}", e)

    try:
        producer.flush()
        logger.info("All messages from '{}' have been sent to topic '{}'.", csv_file_path, topic)
    except Exception as e:
        logger.error("Error flushing producer: {}", e)

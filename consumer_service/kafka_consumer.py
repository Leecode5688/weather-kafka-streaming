from kafka import KafkaConsumer
from config.config import KAFKA_BROKER, KAFKA_TOPIC, BATCH_SIZE, BATCH_TIMEOUT
from mongodb_service.store_to_mongo import connect_to_mongo, close_connection, store_weather_batch
from prometheus_client import Counter, Histogram
import json
import time
import logging

logger = logging.getLogger("consumer_service.kafka_consumer")

# Prometheus Metrics
MESSAGES_CONSUMED = Counter(
    'consumer_messages_consumed_total',
    'Total messages successfully consumed and stored'
)
MESSAGES_LATENCY = Histogram(
    'consumer_message_latency_seconds',
    'End-to-end latency of messages based on timestamp field'
)

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='weather-consumer-group',
        max_poll_records=5000
    )

def flush_batch(consumer, collection, batch):
    """
    Helper function to write batch to MongoDB and commit offsets.
    Also increments Prometheus counters accordingly.
    """
    if not batch:
        return 0

    try:
        if store_weather_batch(collection, batch):
            consumer.commit()
            batch_size = len(batch)
            MESSAGES_CONSUMED.inc(batch_size)
            logger.info(f"Flushed and committed batch of {batch_size} records.")
            return batch_size
        else:
            logger.error("Failed to store batch to MongoDB, offset not committed.")
            return 0
    except Exception as e:
        logger.error(f"Error flushing batch: {e}")
        return 0

def batch_consume_weather_data():
    consumer = create_consumer()
    logger.info(f"Starting to consume messages from Kafka topic: {KAFKA_TOPIC}")

    mongo_client = None
    try:
        mongo_client, collection = connect_to_mongo()
        logger.info("MongoDB connection established and collection ready.")
    except Exception as e:
        logger.error(f"Error establishing MongoDB connection: {e}")
        return

    batch = []
    batch_start_time = time.time()

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            now = time.time()

            # If no messages but there's a batch waiting too long, flush it
            if not messages:
                if batch and (now - batch_start_time > BATCH_TIMEOUT):
                    flushed = flush_batch(consumer, collection, batch)
                    if flushed > 0:
                        batch.clear()
                        batch_start_time = now
                continue

            for topic_partition, records in messages.items():
                for record in records:
                    data = record.value

                    # Handle latency using payload timestamp if present
                    if isinstance(data, dict) and data.get('timestamp'):
                        try:
                            latency_seconds = time.time() - float(data['timestamp'])
                            if latency_seconds >= 0:
                                MESSAGES_LATENCY.observe(latency_seconds)
                        except (TypeError, ValueError):
                            logger.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")

                    # Add to batch if valid
                    if isinstance(data, dict):
                        batch.append(data)
                    else:
                        logger.warning(f"Unexpected message format: {data}")

                    # Flush if batch is full
                    if len(batch) >= BATCH_SIZE:
                        flushed = flush_batch(consumer, collection, batch)
                        if flushed > 0:
                            batch.clear()
                            batch_start_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        # Flush remaining messages on exit
        if batch:
            flushed = flush_batch(consumer, collection, batch)
            if flushed > 0:
                batch.clear()

        consumer.close()
        logger.info("Kafka consumer closed.")
        if mongo_client:
            close_connection(mongo_client)

from kafka import KafkaConsumer
from config.config import KAFKA_BROKER, KAFKA_TOPIC, TIME_OUT
from mongodb_service.store_to_mongo import connect_to_mongo, close_connection, store_weather_batch
import json
import time
import logging

logger = logging.getLogger("consumer_service.kafka_consumer")

BATCH_SIZE = 5
BATCH_TIMEOUT = 50

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )
    
def batch_consume_weather_data():
    consumer = create_consumer()
    logger.info(f"Starting to consume messages from Kafka topic: {KAFKA_TOPIC}")
    
    try:
        collection = connect_to_mongo()
        logger.info("MongoDB connection established and collection ready.")
    except Exception as e:
        logger.error(f"Error establishing MongoDB connection: {e}")
        return

    last_message_time = time.time()
    batch = []
    batch_start_time = time.time()
    
    try:
        while True:
            messages = consumer.poll(timeout_ms=TIME_OUT * 1000)
            if not messages:
                if time.time() - last_message_time > TIME_OUT:
                    logger.info("No new messages received, exiting consumer.")
                    break
                if batch and (time.time() - batch_start_time > BATCH_TIMEOUT):
                    store_weather_batch(collection, batch)
                    batch.clear()
                    batch_start_time = time.time()
                continue
            
            last_message_time = time.time()
            
            for topic_partition, records in messages.items():
                for record in records:
                    data = record.value
                    if isinstance(data, dict):
                        batch.append(data)
                    else:
                        logger.warning(f"Unexpected message format: {data}")

                    #flush if batch is full
                    if len(batch) >= BATCH_SIZE:
                        store_weather_batch(collection, batch)
                        batch.clear()
                        batch_start_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        if batch:
            store_weather_batch(collection, batch)
            batch.clear()
        consumer.close()
        logger.info("Kafka consumer closed.")
        close_connection(collection.client)


from kafka import KafkaConsumer
from config.config import KAFKA_BROKER, KAFKA_TOPIC, TIME_OUT
from mongodb_service.store_to_mongo import connect_to_mongo, close_connection, store_weather_batch
import json
import time
import logging

logger = logging.getLogger("consumer_service.kafka_consumer")

BATCH_SIZE = 500
BATCH_TIMEOUT = 5
# INACTIVITY_TIMEOUT = 30

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='weather-consumer-group'
    )
    
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
    last_message_time = time.time()
    
    message_count = 0
    total_latency = 0
    max_latency = float('-inf')
    min_latency = float('inf')
    
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            now = time.time()
            
            if not messages:
                if batch and (now - batch_start_time > BATCH_TIMEOUT):
                    store_weather_batch(collection, batch)
                    logger.info(f"Flushed batch of {len(batch)} records due to timeout.")
                    batch.clear()
                    batch_start_time = now
                if now - last_message_time > TIME_OUT:
                    logger.info("No new messages received, exiting consumer due to inactivity timeout.")
                    break
                continue

            # if time.time() - last_message_time > TIME_OUT:
            #     logger.info("No new messages received, exiting consumer.")
            #     break
            #     if batch and (time.time() - batch_start_time > BATCH_TIMEOUT):
            #         store_weather_batch(collection, batch)
            #         batch.clear()
            #         batch_start_time = time.time()
            #     continue
            
            # last_message_time = time.time()
            last_message_time = now
            for topic_partition, records in messages.items():
                for record in records:
                    data = record.value
                    
                    if data.get('timestamp'):
                        latency_ms = (time.time() - data['timestamp']) * 1000
                        message_count += 1
                        total_latency += latency_ms
                        if latency_ms > max_latency:
                            max_latency = latency_ms
                        if latency_ms < min_latency:
                            min_latency = latency_ms
                        
                        logger.info(f"Message latency: {latency_ms: .2f} ms")
                        
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
            
        if message_count > 0:
            avg_latency = total_latency / message_count
            logger.info("----Latency Summary-----")
            logger.info(f"Total Message Processed: {message_count}")
            logger.info(f"Average Latency: {avg_latency:.2f} ms")
            logger.info(f"Max Latency:     {max_latency:.2f} ms")
            logger.info(f"Min Latency:     {min_latency:.2f} ms")
        
        consumer.close()
        logger.info("Kafka consumer closed.")
        if mongo_client:
            close_connection(mongo_client)


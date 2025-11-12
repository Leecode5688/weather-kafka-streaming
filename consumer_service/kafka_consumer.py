from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from config.config import (
    KAFKA_BROKER, KAFKA_TOPIC, KAFKA_CONSUMER_DLQ_TOPIC, BATCH_SIZE, BATCH_TIMEOUT
)
from prometheus_client import Counter, Histogram
from pymongo.operations import UpdateOne
from mongodb_service.store_to_mongo import (
    connect_to_mongo_async,
    store_weather_batch_async,
    close_connection
)

import logging
import asyncio
import json
import time


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

MONGO_WRITE_LATENCY = Histogram(
    'consumer_mongo_db_write_latency_seconds',
    'Time spent writing batches to MongoDB'
)

#producer for sending to DLQ
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
    )

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
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
    
async def flush_batch_async(consumer, collection, operations):
    if not operations:
        return 0
    try:
        with MONGO_WRITE_LATENCY.time():
            if not await store_weather_batch_async(collection, operations):
                logger.error("Failed to store batch to MongoDB, offset not committed.")
                return 0
            
            await consumer.commit()
            batch_size = len(operations)
            MESSAGES_CONSUMED.inc(batch_size)
            logger.info(f"Flushed and committed batch of {batch_size} records.")
            return batch_size

    except Exception as e:
        logger.error(f"Error flushing async batch: {e}")
        return 0    
    
def batch_consume_weather_data():
    dlq_producer = create_producer()
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
                    try: 
                        data_str = record.value.decode('utf-8')
                        data = json.loads(data_str)
                        # data = record.value

                        # Handle latency using payload timestamp if present
                        if isinstance(data, dict) and data.get('timestamp'):
                            try:
                                latency_seconds = time.time() - float(data['timestamp'])
                                if latency_seconds >= 0:
                                    MESSAGES_LATENCY.observe(latency_seconds)
                            except (TypeError, ValueError):
                                logger.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")
                        if not isinstance(data, dict):
                            raise ValueError(f"Unexpected message format: {data}")
                    
                        #we can check for required fields here
                        if 'StationId' not in data or 'ObsTime' not in data:
                            raise ValueError(f"Missing required fields in data: {data}")
                    
                        # Add to batch if valid
                        batch.append(data)
                        
                    except Exception as validation_error:   
                        logger.error(f"Validation failed for record, sending to DLQ: {validation_error}") 
                        try:
                            dlq_producer.send(KAFKA_CONSUMER_DLQ_TOPIC, value=record.value)                
                        except Exception as dlq_e:
                            logger.critical(f"Critical error: Failed to send message to DLQ: {KAFKA_CONSUMER_DLQ_TOPIC}, error: {dlq_e}")
                    
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

        dlq_producer.close()
        consumer.close()
        logger.info("Kafka DLQ producer and consumer closed.")
        if mongo_client:
            close_connection(mongo_client)


async def batch_consume_weather_data_async():

    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    )
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC, 
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='weather-consumer-group',
        max_poll_records=5000
    )

    
    logger.info(f"Starting to consume messages from Kafka topic: {KAFKA_TOPIC}")

    mongo_client = None
    try:
        mongo_client, collection = await connect_to_mongo_async()
        await consumer.start()
        await dlq_producer.start()
        logger.info("MongoDB connection established and collection ready.")
    except Exception as e:
        logger.error(f"Error starting services: {e}")
        return

    operations = []
    batch_start_time = time.time()
    
    try: 
        while True: 
            result = await consumer.getmany(
                timeout_ms=1000,
                max_records=BATCH_SIZE
            )

            now = time.time()        
        
            if result:
                for topic_partition, records in result.items():
                    for record in records:
                        try: 
                            data_str = record.value.decode('utf-8')
                            data = json.loads(data_str)

                            # Handle latency using payload timestamp if present
                            if isinstance(data, dict) and data.get('timestamp'):
                                try:
                                    latency_seconds = time.time() - float(data['timestamp'])
                                    if latency_seconds >= 0:
                                        MESSAGES_LATENCY.observe(latency_seconds)
                                except (TypeError, ValueError):
                                    logger.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")
                            if not isinstance(data, dict):
                                raise ValueError(f"Unexpected message format: {data}")
                    
                            #we can check for required fields here
                            if 'StationId' not in data or 'ObsTime' not in data:
                                raise ValueError(f"Missing required fields in data: {data}")
                            
                            filtered = {
                                    "StationName": data.get('StationName'),
                                    "StationId": data.get('StationId'),
                                    "ObservationTime": data.get('ObsTime', {}).get('DateTime'),
                                    "Weather": data.get('WeatherElement', {}).get('Weather'),
                                    "AirTemperature": data.get('WeatherElement', {}).get('AirTemperature'),
                                    "WindSpeed": data.get('WeatherElement', {}).get('WindSpeed')
                            }
                            key = {"StationId": filtered["StationId"], "ObservationTime": filtered["ObservationTime"]}
                            operations.append(UpdateOne(key, {"$setOnInsert": filtered}, upsert=True))    

                            if len(operations) >= BATCH_SIZE:
                                flushed = await flush_batch_async(consumer, collection, operations)
                                if flushed > 0:
                                    operations.clear()
                                    batch_start_time = time.time()
                            
                        except Exception as validation_error:
                            logger.error(f"Validation failed, sending to DLQ: {validation_error}")
                            await dlq_producer.send(KAFKA_CONSUMER_DLQ_TOPIC, value=record.value)
            if operations and (now - batch_start_time > BATCH_TIMEOUT):
                logger.info(f"Batch timeout ({BATCH_TIMEOUT}s), flushing {len(operations)} remaining operations...")         
                flushed = await flush_batch_async(consumer, collection, operations)

                if flushed > 0:
                    operations.clear()
                    batch_start_time = time.time()
                
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        if operations:
            await flush_batch_async(consumer, collection, operations)
        if mongo_client:
            close_connection(mongo_client)
        
        await consumer.stop()
        await dlq_producer.stop()
        logger.info("Kafka async consumer and DLQ producer stopped..")
        
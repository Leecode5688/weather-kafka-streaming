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
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import logging
import asyncio
import random
import json
import time


logger = logging.getLogger("consumer_service.kafka_consumer")
tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()

CUSTOM_BUCKET = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 
    2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 300.0, 1200.0, 3600.0
)

# Prometheus Metrics
MESSAGES_CONSUMED = Counter(
    'consumer_messages_consumed_total',
    'Total messages successfully consumed and stored'
)
TOTAL_E2E_LATENCY = Histogram(
    'consumer_message_latency_seconds',
    'End-to-end latency of messages based on timestamp field',
    buckets=CUSTOM_BUCKET
)

PIPELINE_LAG = Histogram(
    'consumer_pipeline_lag_seconds',
    'Time frome Pipeline processing to MongoDB Write (Consumer Lag)', 
    buckets=CUSTOM_BUCKET
)

MONGO_WRITE_LATENCY = Histogram(
    'consumer_mongo_db_write_latency_seconds',
    'Time spent writing batches to MongoDB', 
    buckets=CUSTOM_BUCKET
)

MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 2000
TARGET_WRITE_LATENCY = 0.5
#linear scaling if fast
ADJUSTMENT_FACTOR_UP = 10
#reduce by 20% if slow
ADJUSTMENT_FACTOR_DOWN = 0.8

MAX_DB_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 15.0

async def flush_batch_async(consumer, collection, operations, total_latencies, pipeline_latencies):
    if not operations:
        return 0, 0.0
    
    with tracer.start_as_current_span("flush_async_batch") as span:
        try:
            span.set_attribute("batch.size", len(operations))
            
            start_time = time.time()
            success = False
            attempt = 0
            current_delay = INITIAL_RETRY_DELAY
            
            while attempt < MAX_DB_RETRIES:
                try: 
                    attempt+=1
                    with MONGO_WRITE_LATENCY.time():
                        success = await store_weather_batch_async(collection, operations)
                    if success:
                        break
                    else:
                        logger.warning(f"MongoDB write returned False (Attempt {attempt}/{MAX_DB_RETRIES})")                    
                except Exception as e:
                    logger.error(f"Error writing to MongoDB (Attempt {attempt}/{MAX_DB_RETRIES}): {e}")
                    span.record_exception(e)

                if attempt < MAX_DB_RETRIES:
                    sleep_time = current_delay * (1 + random.uniform(-0.1, 0.1))
                    logger.info(f"Retrying batch write in {sleep_time:.2f}s...")            
                    await asyncio.sleep(sleep_time)
                    current_delay = min(current_delay * 2, MAX_RETRY_DELAY)
            
            write_duration = time.time() - start_time
            
            #real failure, exausted all retries
            if not success:
                logger.critical("Failed to store batch to MongoDB, offset not committed.")
                span.set_status(Status(StatusCode.ERROR, "Failed to store batch to MongoDB"))
                return 0, write_duration
            
            #success path
            try:
                await consumer.commit()
            except Exception as commit_error:
                logger.critical(f"Failed to commit Kafka offsets: {commit_error}")            
                return 0, write_duration
            
            batch_size = len(operations)
            MESSAGES_CONSUMED.inc(batch_size)

            # record TOTAL latency (Fetch => DB)
            for latency in total_latencies:
                TOTAL_E2E_LATENCY.observe(latency)
            
            #record PIPELINE latency (pipeline => DB)
            for latency in pipeline_latencies:
                PIPELINE_LAG.observe(latency)
            
            logger.info(f"Flushed batch of {batch_size} messages to MongoDB and committed offsets.")
            return batch_size, write_duration
            

        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Error flushing async batch: {e}")
            return 0, 0.0

async def batch_consume_weather_data_async(stop_event: asyncio.Event):

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
    batch_total_latencies = []
    batch_pipeline_latencies = []
    batch_start_time = time.time()
    
    current_batch_size = BATCH_SIZE
    BASE_TIMEOUT = BATCH_TIMEOUT   
    JITTER_PCT = 0.1
    next_flush_deadline = time.time() + BASE_TIMEOUT * (1 + random.uniform(-JITTER_PCT, JITTER_PCT))
    
    try: 
        while not stop_event.is_set():
            result = await consumer.getmany(
                timeout_ms=1000,
                max_records=current_batch_size
            )

            now = time.time()        
        
            if result:
                for topic_partition, records in result.items():
                    for record in records:
                        # Extract trace context from incoming Kafka message headers
                        carrier = {}
                        if record.headers:
                            for key, value in record.headers:
                                if isinstance(value, bytes):
                                    carrier[key] = value.decode('utf-8')
                                else:
                                    carrier[key] = value
                        
                        # Extract the parent context from the carrier
                        parent_context = propagator.extract(carrier=carrier)
                        
                        # Create span with extracted parent context
                        with tracer.start_as_current_span("process_data_message", context=parent_context) as span:
                            try: 
                                span.set_attribute("kafka.topic", record.topic)
                                span.set_attribute("kafka.partition", record.partition)
                                span.set_attribute("kafka.offset", record.offset)
                                
                                current_time = time.time()
                                data_str = record.value.decode('utf-8')
                                data = json.loads(data_str)

                                # handle latency using payload timestamp if present
                                if isinstance(data, dict) and data.get('timestamp'):
                                    try:
                                        total_lag = current_time - float(data['timestamp'])
                                        if total_lag >= 0:
                                            batch_total_latencies.append(total_lag)
                                    except (TypeError, ValueError):
                                        logger.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")
                                
                                if isinstance(data, dict) and data.get('processing_timestamp'):
                                    try:
                                        pipeline_lag = current_time - float(data['processing_timestamp'])
                                        if pipeline_lag >= 0:
                                            batch_pipeline_latencies.append(pipeline_lag)
                                    except (TypeError, ValueError):
                                        logger.warning(f"Invalid processing_timestamp format in message: {data.get('processing_timestamp')}")
                                
                                if not isinstance(data, dict):
                                    raise ValueError(f"Unexpected message format: {data}")
                        
                                #check for required fields
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

                                if len(operations) >= current_batch_size:
                                    flushed, duration = await flush_batch_async(consumer, collection, operations, batch_total_latencies, batch_pipeline_latencies)
                                    
                                    if flushed == 0:
                                        logger.critical("Database unreachable, crashing consumer to trigger restart.")
                                        raise RuntimeError("Database Write Failed")
                                    else:
                                        
                                        if duration < TARGET_WRITE_LATENCY:
                                            current_batch_size = min(MAX_BATCH_SIZE, current_batch_size + ADJUSTMENT_FACTOR_UP)
                                        else:
                                            current_batch_size = max(MIN_BATCH_SIZE, int(current_batch_size * ADJUSTMENT_FACTOR_DOWN))
             
                                                
                                        logger.debug(f"Adjusted batch size to {current_batch_size} based on write duration {duration:.3f}s")
                                        
                                        operations.clear()
                                        batch_total_latencies.clear()
                                        batch_pipeline_latencies.clear()
                                        batch_start_time = time.time()
                                
                            except Exception as validation_error:
                                
                                span.record_exception(validation_error)
                                span.set_status(Status(StatusCode.ERROR, str(validation_error)))
                                logger.error(f"Validation failed, sending to DLQ: {validation_error}")
                                await dlq_producer.send(KAFKA_CONSUMER_DLQ_TOPIC, value=record.value)

            now = time.time()
            if operations and (now > next_flush_deadline):
                logger.info("Batch timeout triggered...")                
                flushed, duration = await flush_batch_async(consumer, collection, operations, batch_total_latencies, batch_pipeline_latencies)
                if flushed == 0:
                    logger.critical("Database unreachable, crashing consumer to trigger restart.")
                    raise RuntimeError("Database Write Failed")
                else:
                    if duration > TARGET_WRITE_LATENCY and current_batch_size > MIN_BATCH_SIZE:
                        current_batch_size = max(MIN_BATCH_SIZE, int(current_batch_size * ADJUSTMENT_FACTOR_DOWN))
                        logger.debug(f"Reduced batch size to {current_batch_size} due to high write latency {duration:.3f}s")
                        
                    operations.clear()
                    batch_total_latencies.clear()
                    batch_pipeline_latencies.clear()

                jitter = random.uniform(-BASE_TIMEOUT * JITTER_PCT, BASE_TIMEOUT * JITTER_PCT)
                next_flush_deadline = time.time() + BASE_TIMEOUT + jitter
                
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        logger.info("Shutdown signal received. Flushing remaining data...")
        if operations:
            await flush_batch_async(consumer, collection, operations, batch_total_latencies, batch_pipeline_latencies)
        if mongo_client:
            close_connection(mongo_client)
        
        await consumer.stop()
        await dlq_producer.stop()
        logger.info("Kafka async consumer and DLQ producer stopped..")
        
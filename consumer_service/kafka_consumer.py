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
import orjson
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


QUEUE_MAX_SIZE = 5000
MAX_DB_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 15.0

async def store_batch_with_retries(collection, operations):
    """
        write to mongoDB with retries
        return true if successful, false if all retries failed
    """
    if not operations:
        return True
    
    attempt = 0
    current_delay = INITIAL_RETRY_DELAY
            
    while attempt < MAX_DB_RETRIES:
        try: 
            attempt+=1
            with MONGO_WRITE_LATENCY.time():
                success = await store_weather_batch_async(collection, operations)
            
            if success:
                return True
            else:
                logger.warning(f"MongoDB write returned False (Attempt {attempt}/{MAX_DB_RETRIES})")                    
        
        except Exception as e:
            logger.error(f"Error writing to MongoDB (Attempt {attempt}/{MAX_DB_RETRIES}): {e}")

        if attempt < MAX_DB_RETRIES:
            sleep_time = current_delay * (1 + random.uniform(-0.1, 0.1))
            logger.info(f"Retrying batch write in {sleep_time:.2f}s...")            
            await asyncio.sleep(sleep_time)
            current_delay = min(current_delay * 2, MAX_RETRY_DELAY)

    logger.critical("Failed to store batch to MongoDB after exhausting retries.")
    return False
            
async def fetch_message_task(consumer, internal_queue, stop_event):
    """
        continuously fetches from Kafka and puts into internal queue
    """
    logger.info("Starting Kafka fetch task... :)")
    try: 
        while not stop_event.is_set():
            result = await consumer.getmany(timeout_ms=1000, max_records=BATCH_SIZE)
            if result:
                for topic_partition, records in result.items():
                    for record in records:
                        #will pause if the queue is full
                        await internal_queue.put(record)
            
            #small yield for other tasks
            await asyncio.sleep(0.01)
    except Exception as e:
        logger.error(f"Error in fetch_message_task: {e}")
    finally:
        #sentinel to tell writer that we are done
        await internal_queue.put(None)
        logger.info("Fetch task stopped...")
        
async def db_writer_task(consumer, collection, dlq_producer, internal_queue, stop_event):
    """
        pulls from queue, processes (validates), accumulates into batches, writes to mongoDB
    """
    logger.info("Starting DB writer task... :)")
    
    operations = []
    total_latencies = []
    pipeline_latencies = []
    batch_deadline = None
    
    while True:
        try: 
            
            if operations and batch_deadline:
                time_remaining = batch_deadline - time.time()
                wait_timeout = max(0, time_remaining)
            else:
                wait_timeout = None
            
            try:
                record = await asyncio.wait_for(internal_queue.get(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                if operations:
                    logger.info(f"Batch timeout of {BATCH_TIMEOUT}s reached! Flushing {len(operations)} records")
                    success = await store_batch_with_retries(collection, operations)
                    
                    if success:
                        
                        MESSAGES_CONSUMED.inc(len(operations))
                        for lat in total_latencies:
                            TOTAL_E2E_LATENCY.observe(lat)
                        for lat in pipeline_latencies:
                            PIPELINE_LAG.observe(lat)
                        await consumer.commit()
                    else:
                        stop_event.set()
                        raise RuntimeError("Database write failed on timeout!!")
                    operations.clear()
                    total_latencies.clear()
                    pipeline_latencies.clear()
                    batch_deadline = None
                continue
                    
            #receive the sentinel, exit
            if record is None:
                if operations:
                    await store_batch_with_retries(collection, operations)
                break
            
            if not operations and batch_deadline is None:
                batch_deadline = time.time() + BATCH_TIMEOUT
            
            records_to_process = [record]
            
            while len(records_to_process) < BATCH_SIZE:
                try:
                    next_rec = internal_queue.get_nowait()
                    if next_rec is None:
                        
                        await internal_queue.put(None)
                        break
                    records_to_process.append(next_rec)
                except asyncio.QueueEmpty:
                    break
            
            current_batch_ops = []
            
            for rec in records_to_process:
                
                carrier = {}
                if rec.headers:
                    for key, value in rec.headers:
                        if isinstance(value, bytes):
                            carrier[key] = value.decode('utf-8')
                        else:
                            carrier[key] = value
                parent_context = propagator.extract(carrier=carrier)
                with tracer.start_as_current_span("process_data_message", context=parent_context) as span:
                    try: 
                        span.set_attribute("kafka.topic", rec.topic)
                        span.set_attribute("kafka.partition", rec.partition)
                        span.set_attribute("kafka.offset", rec.offset)
                        
                        current_time = time.time()
                        data = orjson.loads(rec.value)
                        
                        if isinstance(data, dict) and data.get('timestamp'):
                            try:
                                total_lag = current_time - float(data['timestamp'])
                                if total_lag >= 0:
                                    total_latencies.append(total_lag)
                            except (TypeError, ValueError):
                                logger.warning(f"Invalid timestamp format in message: {data.get('timestamp')}")
                        
                        if isinstance(data, dict) and data.get('processing_timestamp'):
                            try:
                                pipeline_lag = current_time - float(data['processing_timestamp'])
                                if pipeline_lag >= 0:
                                    pipeline_latencies.append(pipeline_lag)
                            except (TypeError, ValueError):
                                logger.warning(f"Invalid processing_timestamp format in message: {data.get('processing_timestamp')}")
                        
                        if not isinstance(data, dict):
                            raise ValueError(f"Unexpected message format: {data}")
                    
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
                        current_batch_ops.append(UpdateOne(key, {"$setOnInsert": filtered}, upsert=True))    
                        
                    except Exception as validation_error:
                        
                        span.record_exception(validation_error)
                        span.set_status(Status(StatusCode.ERROR, str(validation_error)))
                        logger.error(f"Validation failed, sending to DLQ: {validation_error}")
                        
                        try: 
                            await dlq_producer.send(KAFKA_CONSUMER_DLQ_TOPIC, value=rec.value)
                        except Exception as dlq_error:
                            logger.error(f"Failed to send message to DLQ: {dlq_error}")
            
            operations.extend(current_batch_ops)
            
            #write to db if buffer is full
            if len(operations) >= BATCH_SIZE:
                success = await store_batch_with_retries(collection, operations)
                
                if success:
                    MESSAGES_CONSUMED.inc(len(operations))
                    for lat in total_latencies:
                        TOTAL_E2E_LATENCY.observe(lat)
                    for lat in pipeline_latencies:
                        PIPELINE_LAG.observe(lat)

                    await consumer.commit()
                    logger.info(f"Stored batch of {len(operations)} to MongoDB.")
                else:
                    stop_event.set()
                    raise RuntimeError("Database write failed :(")

                operations.clear()
                total_latencies.clear()
                pipeline_latencies.clear()
                batch_deadline = None
        
        except Exception as e:
            logger.error(f"Error in db_writer_task: {e}")
            if stop_event.is_set():
                break
    
    logger.info("DB writer task finished...")
                    
async def batch_consume_weather_data_async(stop_event: asyncio.Event):
    """
    set up connections and start the fetcher and writer tasks
    """
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    )
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC, 
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
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
        
        internal_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        fetcher_task = asyncio.create_task(
            fetch_message_task(consumer, internal_queue, stop_event)
        )
        writer_task = asyncio.create_task(
            db_writer_task(consumer, collection, dlq_producer, internal_queue, stop_event)
        )
        
        await asyncio.gather(fetcher_task, writer_task)
        
    except Exception as e:
        logger.error(f"Fatal error in consumer service: {e}")
    finally:
        logger.info("Shutting down consumer service...")
        
        if mongo_client:
            close_connection(mongo_client)
        await consumer.stop()
        await dlq_producer.stop()
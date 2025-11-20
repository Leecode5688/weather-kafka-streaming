from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from config.config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_RAW_TOPIC, KAFKA_PIPELINE_DLQ_TOPIC, FETCH_INTERVAL, RUN_DURATION
from prometheus_client import Counter

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.context import Context

import asyncio
import logging
import json
import time

#consume from KAFKA_RAW_TOPIC, transform data, and produce to KAFKA_TOPIC

logger = logging.getLogger("producer_service.kafka_producer")
tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()

MESSAGES_PRODUCED = Counter('producer_messages_sent_total', 'Total messages sent to Kafka')

def create_producer():
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )    
    
def create_dlq_producer():
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    )


def create_consumer():
    return AIOKafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='weather-processor-group', 
        max_poll_records=500
    )

async def process_and_send_message(record, producer, dlq_producer):
    # Extract trace context from incoming Kafka message headers
    carrier = {}
    if record.headers:
        for key, value in record.headers:
            if isinstance(value, bytes):
                carrier[key] = value.decode('utf-8')
            else:
                carrier[key] = value
    
    # extract the parent context from the carrier
    parent_context = propagator.extract(carrier=carrier)
    
    # start span with extracted parent context
    with tracer.start_as_current_span("process_raw_message", context=parent_context) as span:
        try:
            span.set_attribute("kafka.topic", record.topic)
            span.set_attribute("kafka.partition", record.partition)
            span.set_attribute("kafka.offset", record.offset)
            
            data_str = record.value.decode('utf-8')
            data = json.loads(data_str)
            
            #validate the data        
            if not isinstance(data, dict):
                raise ValueError(f"Invalid data format received: {data}")
            if 'fetch_timestamp' not in data:
                raise ValueError(f"Missing 'fetch_timestamp' in data: {data}")
            
            data['timestamp'] = data.pop('fetch_timestamp')
            data['processing_timestamp'] = time.time()
            
            # inject trace context into outgoing message headers
            headers = {}
            propagator.inject(headers)
            kafka_headers = [(k, v.encode('utf-8') if isinstance(v, str) else v) for k, v in headers.items()]
            
            await producer.send(KAFKA_TOPIC, value=data, headers=kafka_headers)
            MESSAGES_PRODUCED.inc()
            logger.info(f"Sent processed message to '{KAFKA_TOPIC}'")
            
        except Exception as validation_error:
            
            span.record_exception(validation_error)
            span.set_status(Status(StatusCode.ERROR, str(validation_error)))
            
            logger.error(f"Validation error for message, sending to DLQ: {validation_error}")
            
            try: 
                await dlq_producer.send(KAFKA_PIPELINE_DLQ_TOPIC, value=record.value)
            except Exception as dlq_error:
                logger.error(f"Failed to send message to DLQ: {dlq_error}")


async def send_weather_data():
    producer = create_producer()
    dlq_producer = create_dlq_producer()
    consumer = create_consumer()
    logger.info(f"Starting pipeline service: Consuming from '{KAFKA_RAW_TOPIC}' and producing to '{KAFKA_TOPIC}'")

    await producer.start()
    await dlq_producer.start()
    await consumer.start()
    
    try: 
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=500)
            
            if not result:
                continue
            
            for topic_partition, records in result.items():
                if not records:
                    continue
                
                logger.info(f"Received batch of {len(records)} messages from partition {topic_partition}.")
                
                tasks = []
                for record in records:
                    tasks.append(
                        process_and_send_message(record, producer, dlq_producer)
                    )
                    
                if tasks:
                    await asyncio.gather(*tasks)
                    logger.info(f"Processed and sent {len(tasks)} messages...")
                
                await consumer.commit({topic_partition: records[-1].offset + 1})
                
    except KeyboardInterrupt:
        logger.info("Pipeline service interrupted by user")
    except Exception as e:
        logger.error(f"Error in pipeline service: {e}")
    finally: 
        await producer.stop()
        await dlq_producer.stop()
        await consumer.stop()
        logger.info("Kafka pipeline producer, DLQ producer and consumer closed")       
    
    
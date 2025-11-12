from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from config.config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_RAW_TOPIC, KAFKA_PIPELINE_DLQ_TOPIC, FETCH_INTERVAL, RUN_DURATION
from prometheus_client import Counter
import asyncio
import logging
import json
import time

#consume from KAFKA_RAW_TOPIC, transform data, and produce to KAFKA_TOPIC

logger = logging.getLogger("producer_service.kafka_producer")

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
                
                tasks_to_send = []
                tasks_to_dlq = []
                
                for record in records:
                    try:
                        data_str = record.value.decode('utf-8')
                        data = json.loads(data_str)
                        
                        #validate the data        
                        if not isinstance(data, dict):
                            raise ValueError(f"Invalid data format received: {data}")
                        if 'fetch_timestamp' not in data:
                            raise ValueError(f"Missing 'fetch_timestamp' in data: {data}")
                        
                        data['timestamp'] = data.pop('fetch_timestamp')
                        data['processing_timestamp'] = time.time()
                        
                        # try:
                        #     await producer.send(KAFKA_TOPIC, value=data)
                        #     MESSAGES_PRODUCED.inc()
                        # except Exception as e:
                        #     logger.error(f"Failed to send processed data to kafka: {e}")
                        
                        tasks_to_send.append(
                            producer.send(KAFKA_TOPIC, value=data)
                        )
                        
                    except Exception as validation_error:
                        logger.error(f"Validation error for message, sending to DLQ: {validation_error}")
                        tasks_to_dlq.append(
                            dlq_producer.send(KAFKA_PIPELINE_DLQ_TOPIC, value=record.value)
                        )
                        # try:
                        #     await dlq_producer.send(KAFKA_PIPELINE_DLQ_TOPIC, value=record.value)
                        # except Exception as dlq_e:
                        #     logger.critical(f"Critical error: Failed to send message to DLQ: {KAFKA_PIPELINE_DLQ_TOPIC}")
                
                if tasks_to_send:
                    await asyncio.gather(*tasks_to_send)
                    MESSAGES_PRODUCED.inc(len(tasks_to_send))
                    logger.info(f"Sent {len(tasks_to_send)} processed messages to '{KAFKA_TOPIC}'")
                
                if tasks_to_dlq:
                    await asyncio.gather(*tasks_to_dlq)
                
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
    
    
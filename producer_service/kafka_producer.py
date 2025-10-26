from kafka import KafkaProducer, KafkaConsumer
from fetcher_service.fetch_weather import get_weather
from config.config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_RAW_TOPIC, FETCH_INTERVAL, RUN_DURATION
from prometheus_client import Counter
import logging
import json
import time
import os

#consume from KAFKA_RAW_TOPIC, transform data, and produce to KAFKA_TOPIC

logger = logging.getLogger("producer_service.kafka_producer")

MESSAGES_PRODUCED = Counter('producer_messages_sent_total', 'Total messages sent to Kafka')
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    
def create_consumer():
    return KafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-processor-group'
    )
    
def send_weather_data():
    """
    this function runs a stream processing loop: 
    consume from KAFKA_RAW_TOPIC => Transform => Produce to KAFKA_TOPIC
    """
    producer = create_producer()
    consumer = create_consumer()
    logger.info(f"Starting pipeline service: Consuming from '{KAFKA_RAW_TOPIC}' and producing to '{KAFKA_TOPIC}'")

    # start_time = time.time()
    # end_time = start_time + RUN_DURATION    
    
    # while time.time() < end_time:

    try: 
        for message in consumer:
            data = message.value
            
            if not isinstance(data, dict):
                logger.error(f"Invalid data format received: {data}")
                continue    
            
            #for future extensibility, we transform the data here
            #for now we just rename the timestamp
            if 'fetch_timestamp' in data:
                data['timestamp'] = data.pop('fetch_timestamp')
            
            #add a processing timestamp
            data['processing_timestamp'] = time.time()
            
            try: 
                producer.send(KAFKA_TOPIC, value=data)
                MESSAGES_PRODUCED.inc()
            except Exception as e:
                logger.error(f"Failed to send processed data to kafka: {e}")
    
    except KeyboardInterrupt:
        logger.info("Pipeline service interrupted by user")
    except Exception as e:
        logger.error(f"Error in pipeline service: {e}")
    finally: 
        producer.close()
        consumer.close()
        logger.info("Kafka pipeline producer and consumer closed")       

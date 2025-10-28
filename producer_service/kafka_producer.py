from kafka import KafkaProducer, KafkaConsumer
from fetcher_service.fetch_weather import get_weather
from config.config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_RAW_TOPIC, KAFKA_PIPELINE_DLQ_TOPIC, FETCH_INTERVAL, RUN_DURATION
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

#producer for DLQ, send raw bytes, no serialization
def create_dlq_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9)
    )


def create_consumer():
    return KafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
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
    dlq_producer = create_dlq_producer()
    
    consumer = create_consumer()
    logger.info(f"Starting pipeline service: Consuming from '{KAFKA_RAW_TOPIC}' and producing to '{KAFKA_TOPIC}'")

    try: 
        for message in consumer:
            try:
                
                data_str = message.value.decode('utf-8')
                data = json.loads(data_str)
                
                #validate the data        
                if not isinstance(data, dict):
                    raise ValueError(f"Invalid data format received: {data}")
                if 'fetch_timestamp' not in data:
                    raise ValueError(f"Missing 'fetch_timestamp' in data: {data}")
            
                #for future extensibility, we transform the data here
                #for now we just rename the timestamp
                data['timestamp'] = data.pop('fetch_timestamp')
            
                #add a processing timestamp
                data['processing_timestamp'] = time.time()
            
                try: 
                    producer.send(KAFKA_TOPIC, value=data)
                    MESSAGES_PRODUCED.inc()
                except Exception as e:
                    logger.error(f"Failed to send processed data to kafka: {e}")
            
            except Exception as validation_error:
                logger.error(f"Validation error for message, sending to DLQ: {validation_error}")
                try: 
                    dlq_producer.send(KAFKA_PIPELINE_DLQ_TOPIC, value=message.value)
                except Exception as dlq_e:
                    logger.critical(f"Critical error: Failed to send message to DLQ: {KAFKA_PIPELINE_DLQ_TOPIC}")
                    
    except KeyboardInterrupt:
        logger.info("Pipeline service interrupted by user")
    except Exception as e:
        logger.error(f"Error in pipeline service: {e}")
    finally: 
        producer.close()
        dlq_producer.close()
        consumer.close()
        logger.info("Kafka pipeline producer, DLQ producer and consumer closed")       

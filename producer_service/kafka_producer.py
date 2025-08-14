from kafka import KafkaProducer
from fetcher_service.fetch_weather import get_weather
from config.config import KAFKA_BROKER, KAFKA_TOPIC, FETCH_INTERVAL, RUN_DURATION
import logging
import json
import time
import os

logger = logging.getLogger(__name__)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
def send_weather_data():
    producer = create_producer()
    start_time = time.time()
    end_time = start_time + RUN_DURATION    
    
    while time.time() < end_time:
        weather_data = get_weather()
        if not weather_data:
            logger.info("No weather data fetched this interval")
            time.sleep(FETCH_INTERVAL)
            continue
        
        #send data to kafka
        #note that the data would be duplicated due to the api update frequency
        for entry in weather_data:
            try:
                producer.send(KAFKA_TOPIC, value=entry)
            except Exception as e:
                logger.error(f"Failed to send data to Kafka: {e}")
        
        producer.flush()
        time.sleep(FETCH_INTERVAL)
    
    producer.close()
    logger.info("Kafka producer closed")

if __name__ == "__main__":
    send_weather_data()
    
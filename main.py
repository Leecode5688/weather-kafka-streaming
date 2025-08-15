from fetcher_service.fetch_weather import fetch_weather_data
from producer_service.kafka_producer import send_weather_data
from consumer_service.kafka_consumer import consume_weather_data
from mongodb_service.store_to_mongo import store_weather_data, store_weather_batch
import threading
import logging
import time
import os

os.makedirs("logs", exist_ok = True)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

file_Handler = logging.FileHandler("logs/pipeline.log")
file_Handler.setLevel(logging.INFO)

console_Handler = logging.StreamHandler()
console_Handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_Handler.setFormatter(formatter)
console_Handler.setFormatter(formatter)

logger.addHandler(file_Handler)
logger.addHandler(console_Handler)

def run_pipeline():
    logger.info("Starting the data pipeline :)")
    producer_thread = threading.Thread(target=send_weather_data, name="KafkaProducerThread")
    consumer_thread = threading.Thread(target=consume_weather_data, name="KafkaConsumerThread")

    producer_thread.start()
    consumer_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
    finally:
        producer_thread.join()
        consumer_thread.join()
        logger.info("Data pipeline completed :)")

if __name__ == "__main__":
    run_pipeline()
    logger.info("Weather data pipeline should be running :)")
    

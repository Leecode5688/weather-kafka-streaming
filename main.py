from data_ingestion.fetch_weather import fetch_weather_data
from kafka_setup.kafka_producer import send_weather_data
from mongodb_setup.store_to_mongo import consume_weather_data as store_to_mongo
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

logger = logging.getLogger(__name__)

WEATHER_DATA_PATH = "data_ingestion/weather_data.json"
TIME_OUT = 60

def wait_for_file_ready(file_path):
    logger.info(f"Waiting for {file_path} to be ready ;)")
    start_time = time.time()
    while time.time()-start_time < TIME_OUT:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            logger.info(f"{file_path} is ready :)")
            return True
        time.sleep(1)
    logger.warning(f"{file_path} is not ready within the timeout period :(")
    return False

def run_pipeline():
    logger.info("Starting the data pipeline :)")
    fetch_weather_data()
    if not wait_for_file_ready(WEATHER_DATA_PATH):
        logger.warning("Weather data not ready. Exiting pipeline.")
        return

    logger.info("Weather data fetched. Sending to Kafka :)")
    send_thread = threading.Thread(target=send_weather_data)
    logger.info("Consume & store weather data to MongoDB :)")
    consume_and_store_thread = threading.Thread(target=store_to_mongo)
    
    send_thread.start()
    consume_and_store_thread.start()
    
    send_thread.join()
    consume_and_store_thread.join()
    logger.info("Data pipeline completed :)")
    
if __name__ == "__main__":
    run_pipeline()
    logger.info("Weather data pipeline should be running :)")
    

import logging
import time
import os
from .kafka_producer import send_weather_data
from config.logging_config import setup_logger

# if not os.path.exists("../logs"):
#     os.makedirs("../logs")

# logger = logging.getLogger("producer_service")
# logger.setLevel(logging.INFO)

# if logger.hasHandlers():
#     logger.handlers.clear()

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# file_handler = logging.FileHandler("../logs/producer.log")
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# logger.addHandler(console_handler)

logger = setup_logger("producer_service", "../logs/producer.log")

def run_producer():
    logger.info("Starting the producer service...")
    try:
        send_weather_data()
    except KeyboardInterrupt:
        logger.info("Producer service stopped...")
        
if __name__ == "__main__":
    run_producer()
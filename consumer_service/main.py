import logging
import os
from .kafka_consumer import batch_consume_weather_data
from config.logging_config import setup_logger

if not os.path.exists("../logs"):
    os.makedirs("../logs")

# logger = logging.getLogger("consumer_service")
# logger.setLevel(logging.INFO)

# if logger.hasHandlers():
#     logger.handlers.clear()

# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler = logging.FileHandler("../logs/consumer.log")
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# logger.addHandler(console_handler)

logger = setup_logger("consumer_service", "../logs/consumer.log")

def run_consumer():
    logger.info("Starting the consumer service...")
    try:
        batch_consume_weather_data()
    except KeyboardInterrupt:
        logger.info("Consumer service stopped...")

if __name__ == "__main__":
    run_consumer()
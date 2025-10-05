import logging
import os
from .kafka_consumer import batch_consume_weather_data
from config.logging_config import setup_logger

if not os.path.exists("../logs"):
    os.makedirs("../logs")

logger = setup_logger("consumer_service", "../logs/consumer.log")

def run_consumer():
    logger.info("Starting the consumer service...")
    try:
        batch_consume_weather_data()
    except KeyboardInterrupt:
        logger.info("Consumer service stopped...")

if __name__ == "__main__":
    run_consumer()
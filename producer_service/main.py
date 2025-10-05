import logging
import time
import os
from .kafka_producer import send_weather_data
from config.logging_config import setup_logger

logger = setup_logger("producer_service", "../logs/producer.log")

def run_producer():
    logger.info("Starting the producer service...")
    try:
        send_weather_data()
    except KeyboardInterrupt:
        logger.info("Producer service stopped...")
        
if __name__ == "__main__":
    run_producer()
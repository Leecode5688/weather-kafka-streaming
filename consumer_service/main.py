import logging
import os
from .kafka_consumer import batch_consume_weather_data
from config.logging_config import setup_logger
from config.config import CONSUMER_METRICS_PORT
from prometheus_client import start_http_server
import threading

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
    #add a daemon thread to run prometheus server
    threading.Thread(target=lambda: start_http_server(CONSUMER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {CONSUMER_METRICS_PORT}")
    
    run_consumer()
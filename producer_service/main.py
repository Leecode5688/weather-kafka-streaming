import logging
import time
import os
import threading
from .kafka_producer import send_weather_data
from config.logging_config import setup_logger
from config.config import PRODUCER_METRICS_PORT
from prometheus_client import start_http_server

logger = setup_logger("producer_service", "../logs/producer.log")

def run_producer():
    logger.info("Starting the producer service...")
    try:
        send_weather_data()
    except KeyboardInterrupt:
        logger.info("Producer service stopped...")
        
if __name__ == "__main__":
    threading.Thread(target=lambda: start_http_server(PRODUCER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {PRODUCER_METRICS_PORT}")
    run_producer()
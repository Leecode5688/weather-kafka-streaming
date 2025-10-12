import logging
import os
import time
import threading
from .fetch_weather import get_weather
from config.config import FETCH_INTERVAL, FETCHER_METRICS_PORT
from config.logging_config import setup_logger
from prometheus_client import start_http_server

logger = setup_logger("fetcher_service", "../logs/fetcher.log")
def run_fetcher():
    logger.info("Starting fetcher service...")
    try:
        while True:
            weather_data = get_weather()
            if weather_data:
                logger.info(f"Fetched {len(weather_data)} weather records")
                # Optional: print first record for verification
                logger.debug(f"Sample data: {weather_data[0]}")
            else:
                logger.warning("No weather data fetched this interval")

            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Fetcher service stopped by user.")


if __name__ == "__main__":
    # Start Prometheus metrics server
    threading.Thread(target=lambda: start_http_server(FETCHER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {FETCHER_METRICS_PORT}")

    run_fetcher()

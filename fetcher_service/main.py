import logging
import os
import time
from .fetch_weather import get_weather
from config.config import FETCH_INTERVAL
from config.logging_config import setup_logger

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
    run_fetcher()

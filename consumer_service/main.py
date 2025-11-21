import logging
import asyncio
import threading
import os
import signal

from config.telemetry import setup_otel
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
    
setup_otel("weather-consumer")
AIOKafkaInstrumentor().instrument()
PymongoInstrumentor().instrument()

from config.logging_config import setup_logger
from config.config import CONSUMER_METRICS_PORT
from prometheus_client import start_http_server
from .kafka_consumer import batch_consume_weather_data_async


if not os.path.exists("../logs"):
    os.makedirs("../logs")

logger = setup_logger("consumer_service", "logs/consumer.log")

async def run_consumer_async(stop_event: asyncio.Event):
    logger.info("Starting the async consumer service...")
    try: 
        await batch_consume_weather_data_async(stop_event)
    except Exception as e: 
        logger.error(f"Consumer service failed: {e}")

if __name__ == "__main__":
    
    #add a daemon thread to run prometheus server
    threading.Thread(target=lambda: start_http_server(CONSUMER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {CONSUMER_METRICS_PORT}")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: stop_event.set())
    try: 
        loop.run_until_complete(run_consumer_async(stop_event))
    finally:
        loop.close()
        logger.info("Consumer service shutdown complete.")
    

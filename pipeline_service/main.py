import threading
import logging
import asyncio
import time
import os
import signal

from config.telemetry import setup_otel
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor

setup_otel("weather-pipeline")
AIOKafkaInstrumentor().instrument()


from config.logging_config import setup_logger
from config.config import PRODUCER_METRICS_PORT
from prometheus_client import start_http_server
from .kafka_producer import send_weather_data

logger = setup_logger("producer_service", "logs/producer.log")

async def run_producer(stop_event: asyncio.Event):
    logger.info("Starting the producer service...")
    try:
        await send_weather_data(stop_event)
    except Exception as e:
        logger.info(f"Producer service stopped: {e}")
        
if __name__ == "__main__":
    
    threading.Thread(target=lambda: start_http_server(PRODUCER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {PRODUCER_METRICS_PORT}")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    try: 
        loop.run_until_complete(run_producer(stop_event))
    finally: 
        loop.close()
    
    
